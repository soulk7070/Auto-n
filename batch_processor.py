#!/usr/bin/env python3
"""
ComfyUI Auto Batch Processor
Automated batch processing for ComfyUI with custom prompt format.

FORMAT PROMPT:
[∆{workflow}•count∆ ¥positive¥ §§negative§§]
atau tanpa negative:
[∆{workflow}•count∆ ¥positive¥]

Contoh:
[∆{landscape-m}•2∆ ¥beautiful portrait¥ §§ugly, blurry§§]
"""

import json
import requests
import uuid
import time
import os
import re
import random
import threading
from datetime import datetime
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed


class ComfyUIAutoBatch:
    def __init__(self, server_address="127.0.0.1:8188", max_concurrent=3, retry_attempts=3):
        self.server_address = server_address
        self.client_id = str(uuid.uuid4())
        self.max_concurrent = max_concurrent
        self.retry_attempts = retry_attempts
        self.session = requests.Session()

        self._setup_logging()

        # Semua workflow yang didukung (pastikan file JSON ada di folder workflows/)
        self.all_workflows = [
            'landscape-m', 'portrait-m', 'square-m',
            'landscape-dl', 'portrait-dl', 'square-dl',
            'vector', 'vector-color', 'flux-nsfw',
            'lora1', 'lora2', 'lora3'
        ]

        self._workflow_cache: Dict[str, Dict[str, Any]] = {}
        self.available_workflows: List[str] = []

        self.stats = {
            'total_queued': 0,
            'completed': 0,
            'failed': 0,
            'retries': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }

        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.job_lock = threading.Lock()

    # ----------------- LOGGING -----------------
    def _setup_logging(self):
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        self.logger = logging.getLogger(f"ComfyUI_Auto_{self.client_id[:8]}")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            console = logging.StreamHandler()
            console.setFormatter(fmt)

            try:
                from logging.handlers import RotatingFileHandler
                fh = RotatingFileHandler(log_dir / 'batch_process.log', maxBytes=10 * 1024 * 1024, backupCount=3)
                fh.setFormatter(fmt)
            except ImportError:
                fh = logging.FileHandler(log_dir / 'batch_process.log')
                fh.setFormatter(fmt)

            self.logger.addHandler(fh)
            self.logger.addHandler(console)

    # ----------------- WORKFLOWS -----------------
    def scan_available_workflows(self) -> List[str]:
        workflows_dir = Path('workflows')
        if not workflows_dir.exists():
            self.logger.error("❌ Workflows folder not found!")
            return []

        available = []
        for wf in self.all_workflows:
            if (workflows_dir / f"{wf}.json").exists():
                available.append(wf)

        self.available_workflows = available
        self.logger.info(f"📁 Available workflows: {len(available)}/{len(self.all_workflows)}")
        if available:
            self.logger.info(f"🎯 Ready: {', '.join(available)}")
        else:
            self.logger.error("❌ No workflow files found in workflows/ folder!")
        return available

    def load_workflow(self, workflow_type: str) -> Optional[Dict[str, Any]]:
        if workflow_type in self._workflow_cache:
            return self._workflow_cache[workflow_type].copy()

        path = Path('workflows') / f"{workflow_type}.json"
        if not path.exists():
            return None

        try:
            with open(path, 'r', encoding='utf-8') as f:
                wf = json.load(f)
            self._workflow_cache[workflow_type] = wf
            return wf.copy()
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON in {workflow_type}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Error loading {workflow_type}: {e}")
            return None

    # ----------------- PROMPT PARSER -----------------
    def parse_prompt_file(self, filepath: str) -> List[Dict[str, Any]]:
        """
        Parse prompt file dengan format:
        [∆{workflow}•count∆ ¥positive¥ §§negative§§]  (negative opsional)
        """
        prompts: List[Dict[str, Any]] = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            # Ambil setiap blok di dalam [ ... ]
            tasks = re.findall(r'\[[^\]]+\]', content)

            for task_idx, task in enumerate(tasks, 1):
                try:
                    # {workflow}•count
                    workflow_matches = re.findall(r'\{([^}]+)\}•(\d+)', task)

                    # ¥positive¥ dan opsional §§negative§§
                    prompt_match = re.search(r'¥([^¥]+)¥(?:\s*§§([^§]+)§§)?', task)

                    if not workflow_matches or not prompt_match:
                        self.logger.warning(f"Task {task_idx}: Invalid format - {task[:50]}...")
                        continue

                    positive = prompt_match.group(1).strip()
                    negative = prompt_match.group(2).strip() if prompt_match.group(2) else ""

                    ratios, total = [], 0
                    for wf_name, cnt_str in workflow_matches:
                        wf_name = wf_name.strip()
                        cnt = int(cnt_str)
                        if wf_name in self.all_workflows and cnt > 0:
                            ratios.append({'type': wf_name, 'count': cnt})
                            total += cnt
                        else:
                            self.logger.warning(f"Task {task_idx}: Invalid workflow '{wf_name}' or count '{cnt}'")

                    if ratios and total > 0:
                        prompts.append({
                            'positive_text': positive,
                            'negative_text': negative,
                            'ratios': ratios,
                            'task_num': task_idx,
                            'total_count': total
                        })
                except Exception as e:
                    self.logger.warning(f"Task {task_idx}: Parse error - {e}")
                    continue

        except FileNotFoundError:
            self.logger.error(f"❌ Prompt file not found: {filepath}")
            return []
        except Exception as e:
            self.logger.error(f"❌ Error reading prompt file: {e}")
            return []

        total_generations = sum(p['total_count'] for p in prompts)
        self.logger.info(f"✅ Parsed {len(prompts)} tasks, {total_generations} total generations")
        return prompts

    # ----------------- UPDATE PROMPT KE WORKFLOW -----------------
    def update_workflow_prompt(
        self,
        workflow: Dict[str, Any],
        positive_text: str,
        negative_text: str = ""
    ) -> Dict[str, Any]:
        """
        Isi node CLIPTextEncode untuk positive & negative.
        Deteksi node negative via label/title/meta berisi 'neg' atau 'negative'.
        Juga randomize seed pada KSampler/KSamplerAdvanced jika tersedia.
        """
        pos_set = 0
        neg_set = 0

        for _node_id, node in workflow.items():
            if not isinstance(node, dict):
                continue

            class_type = node.get('class_type')
            inputs = node.get('inputs', {})

            # Random seed
            if class_type in ['KSampler', 'KSamplerAdvanced'] and 'seed' in inputs:
                inputs['seed'] = random.randint(1, 2**32 - 1)

            # Set text
            if class_type == 'CLIPTextEncode' and 'text' in inputs:
                # Coba ambil label untuk identifikasi negative
                label = ""
                meta = node.get('_meta') or {}
                if isinstance(meta, dict):
                    label = (meta.get('title') or meta.get('label') or "").lower()
                label = (node.get('label') or label or "").lower()

                if 'neg' in label or 'negative' in label:
                    inputs['text'] = negative_text
                    neg_set += 1
                else:
                    # Asumsikan node pertama adalah positive jika tidak ada label
                    if pos_set == 0:
                        inputs['text'] = positive_text
                        pos_set += 1
                    else:
                        # Biarkan node CLIPTextEncode lain apa adanya
                        pass

        if pos_set == 0:
            self.logger.warning("⚠️  Tidak menemukan encoder positif (CLIPTextEncode) untuk diisi.")
        if negative_text and neg_set == 0:
            self.logger.warning("⚠️  Negative prompt ada, tapi tidak menemukan encoder negatif.\n"
                                "   → Pastikan workflow punya CLIPTextEncode berlabel 'negative'.")

        return workflow

    # ----------------- QUEUE & MONITOR -----------------
    def queue_prompt_with_retry(self, workflow: Dict[str, Any], attempt: int = 1) -> Optional[str]:
        try:
            payload = {"prompt": workflow, "client_id": self.client_id}
            resp = self.session.post(f"http://{self.server_address}/prompt", json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if 'prompt_id' in data:
                return data['prompt_id']
            self.logger.error(f"❌ No prompt_id in response: {data}")
            return None
        except requests.exceptions.RequestException as e:
            if attempt <= self.retry_attempts:
                wait = 2 ** attempt
                self.logger.warning(f"⚠️  Queue attempt {attempt} failed, retrying in {wait}s: {e}")
                time.sleep(wait)
                self.stats['retries'] += 1
                return self.queue_prompt_with_retry(workflow, attempt + 1)
            self.logger.error(f"❌ Queue failed after {self.retry_attempts} attempts: {e}")
            return None

    def wait_for_completion(self, prompt_id: str, timeout: int = 600) -> bool:
        start = time.time()
        interval = 3
        max_interval = 15

        while time.time() - start < timeout:
            try:
                r = self.session.get(f"http://{self.server_address}/history/{prompt_id}", timeout=10)
                if r.status_code == 200:
                    hist = r.json()
                    if prompt_id in hist:
                        return True
                elif r.status_code == 404:
                    q = self.session.get(f"http://{self.server_address}/queue", timeout=10)
                    if q.status_code == 200:
                        qd = q.json()
                        in_queue = any(
                            item[1]['prompt_id'] == prompt_id
                            for item in qd.get('queue_running', []) + qd.get('queue_pending', [])
                        )
                        if not in_queue:
                            self.logger.error(f"❌ Prompt {prompt_id} not found in queue or history")
                            return False

                elapsed = time.time() - start
                if elapsed > 120:
                    interval = min(max_interval, interval + 1)
                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"❌ Error checking {prompt_id}: {e}")
                time.sleep(5)

        self.logger.error(f"⏰ Timeout waiting for {prompt_id} after {timeout}s")
        return False

    # ----------------- EXECUTION -----------------
    def process_single_generation(self, positive_prompt: str, negative_prompt: str, workflow_type: str, gen_index: str) -> Dict[str, Any]:
        job_id = f"{workflow_type}_{gen_index}_{int(time.time())}"

        # Cek ketersediaan workflow
        if workflow_type not in self.available_workflows:
            self.stats['skipped'] += 1
            self.logger.warning(f"⏭️  Skipped {job_id} - workflow not available")
            return {'status': 'skipped', 'job_id': job_id, 'workflow_type': workflow_type, 'reason': 'workflow_not_available'}

        try:
            wf = self.load_workflow(workflow_type)
            if not wf:
                self.stats['skipped'] += 1
                return {'status': 'skipped', 'job_id': job_id, 'workflow_type': workflow_type, 'reason': 'workflow_load_failed'}

            wf = self.update_workflow_prompt(wf, positive_prompt, negative_prompt)

            prompt_id = self.queue_prompt_with_retry(wf)
            if not prompt_id:
                self.stats['failed'] += 1
                return {'status': 'failed', 'job_id': job_id, 'workflow_type': workflow_type, 'error': 'queue_failed'}

            with self.job_lock:
                self.active_jobs[prompt_id] = {'job_id': job_id, 'workflow_type': workflow_type, 'start_time': time.time()}

            self.stats['total_queued'] += 1
            self.logger.info(f"🚀 Queued {job_id} (ID: {prompt_id})")

            if self.wait_for_completion(prompt_id):
                with self.job_lock:
                    if prompt_id in self.active_jobs:
                        duration = time.time() - self.active_jobs[prompt_id]['start_time']
                        del self.active_jobs[prompt_id]
                self.stats['completed'] += 1
                self.logger.info(f"✅ Completed {job_id} in {duration:.1f}s")
                return {'status': 'completed', 'job_id': job_id, 'prompt_id': prompt_id, 'workflow_type': workflow_type, 'duration': duration}
            else:
                with self.job_lock:
                    if prompt_id in self.active_jobs:
                        del self.active_jobs[prompt_id]
                self.stats['failed'] += 1
                return {'status': 'timeout', 'job_id': job_id, 'prompt_id': prompt_id, 'workflow_type': workflow_type}

        except Exception as e:
            self.stats['failed'] += 1
            self.logger.error(f"❌ Error in {job_id}: {e}")
            return {'status': 'error', 'job_id': job_id, 'error': str(e), 'workflow_type': workflow_type}

    def process_prompts(self, prompt_file: str) -> bool:
        self.logger.info("🎯 ComfyUI Auto Batch Processor Started")

        if not self.test_connection():
            self.logger.error("❌ Cannot connect to ComfyUI server!")
            return False

        available = self.scan_available_workflows()
        if not available:
            self.logger.error("❌ No workflows available!")
            return False

        prompts = self.parse_prompt_file(prompt_file)
        if not prompts:
            self.logger.error("❌ No valid prompts found!")
            return False

        total_generations = sum(p['total_count'] for p in prompts)
        self.logger.info(f"🎯 Starting batch: {len(prompts)} tasks, {total_generations} generations")
        self.logger.info(f"⚙️  Max concurrent: {self.max_concurrent}")

        self.stats['start_time'] = time.time()

        # Susun daftar job
        tasks = []
        for p in prompts:
            pos = p['positive_text']
            neg = p['negative_text']
            tnum = p['task_num']
            for ratio in p['ratios']:
                wf_type = ratio['type']
                count = ratio['count']
                for i in range(count):
                    tasks.append((pos, neg, wf_type, f"T{tnum}_{wf_type}_{i+1}"))

        results = []
        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                future_map = {
                    executor.submit(self.process_single_generation, pos, neg, wf, idx): (pos, neg, wf, idx)
                    for (pos, neg, wf, idx) in tasks
                }
                for future in as_completed(future_map):
                    result = future.result()
                    results.append(result)

                    completed = len([r for r in results if r['status'] == 'completed'])
                    skipped = len([r for r in results if r['status'] == 'skipped'])
                    progress = (len(results) / total_generations) * 100
                    self.logger.info(f"📊 Progress: {len(results)}/{total_generations} ({progress:.1f}%) | ✅{completed} ⏭️{skipped}")
        except KeyboardInterrupt:
            self.logger.info("🛑 Interrupted by user")
            return False
        except Exception as e:
            self.logger.error(f"💥 Execution error: {e}")
            return False
        finally:
            self.stats['end_time'] = time.time()
            self.print_final_stats()

        return True

    # ----------------- STATS -----------------
    def print_final_stats(self):
        duration = self.stats['end_time'] - self.stats['start_time']
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📈 BATCH PROCESSING COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f}min)")
        self.logger.info(f"🚀 Queued: {self.stats['total_queued']}")
        self.logger.info(f"✅ Completed: {self.stats['completed']}")
        self.logger.info(f"❌ Failed: {self.stats['failed']}")
        self.logger.info(f"⏭️  Skipped: {self.stats['skipped']}")
        self.logger.info(f"🔄 Retries: {self.stats['retries']}")
        if self.stats['completed'] > 0:
            avg = duration / self.stats['completed']
            self.logger.info(f"⚡ Avg per Generation: {avg:.1f}s")
        attempted = self.stats['total_queued']
        if attempted > 0:
            success = (self.stats['completed'] / attempted) * 100
            self.logger.info(f"📊 Success Rate: {success:.1f}%")
        self.logger.info("=" * 60)

    # ----------------- CONNECTION TEST -----------------
    def test_connection(self) -> bool:
        try:
            r = self.session.get(f"http://{self.server_address}/system_stats", timeout=10)
            if r.status_code == 200:
                q = self.session.get(f"http://{self.server_address}/queue", timeout=5)
                if q.status_code == 200:
                    self.logger.info("✅ ComfyUI connection successful")
                    return True
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ Cannot connect to ComfyUI at http://{self.server_address}")
        except requests.exceptions.Timeout:
            self.logger.error("❌ Connection timeout to ComfyUI")
        except Exception as e:
            self.logger.error(f"❌ Connection test failed: {e}")
        return False


# ----------------- ENTRY POINT -----------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python comfyuiauto.py <prompt_file.txt> [max_concurrent] [server_address]")
        print("Example: python comfyuiauto.py prompts/my_prompts.txt 3 127.0.0.1:8188")
        sys.exit(1)

    prompt_file = sys.argv[1]
    max_concurrent = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    server_address = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:8188"

    if not os.path.exists(prompt_file):
        print(f"❌ File not found: {prompt_file}")
        sys.exit(1)

    processor = ComfyUIAutoBatch(server_address=server_address, max_concurrent=max_concurrent)

    try:
        success = processor.process_prompts(prompt_file)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)
