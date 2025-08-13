#!/usr/bin/env python3
"""
ComfyUI Auto Batch Processor
Automated batch processing for ComfyUI with custom prompt format
Format: [∆{workflow}•count∆ ¥prompt¥]
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
        """
        ComfyUI Auto Batch Processor
        
        Args:
            server_address: ComfyUI server address
            max_concurrent: Maximum concurrent generations
            retry_attempts: Number of retry attempts for failed generations
        """
        self.server_address = server_address
        self.client_id = str(uuid.uuid4())
        self.max_concurrent = max_concurrent
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        
        # Setup logging
        self._setup_logging()
        
        # All supported workflow types (12 total)
        self.all_workflows = [
            'landscape-m', 'portrait-m', 'square-m',
            'landscape-dl', 'portrait-dl', 'square-dl', 
            'vector', 'vector-color', 'flux-nsfw',
            'lora1', 'lora2', 'lora3'
        ]
        
        # Cache for loaded workflows
        self._workflow_cache = {}
        self.available_workflows = []
        
        # Statistics
        self.stats = {
            'total_queued': 0,
            'completed': 0,
            'failed': 0,
            'retries': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Active jobs tracking
        self.active_jobs = {}
        self.job_lock = threading.Lock()

    def _setup_logging(self):
        """Setup logging with rotation"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(f"ComfyUI_Auto_{self.client_id[:8]}")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            
            # File handler
            try:
                from logging.handlers import RotatingFileHandler
                file_handler = RotatingFileHandler(
                    log_dir / 'batch_process.log',
                    maxBytes=10*1024*1024,  # 10MB
                    backupCount=3
                )
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except ImportError:
                # Fallback to basic file handler
                file_handler = logging.FileHandler(log_dir / 'batch_process.log')
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            
            self.logger.addHandler(console_handler)

    def scan_available_workflows(self) -> List[str]:
        """Scan workflows folder for available JSON files"""
        workflows_dir = Path('workflows')
        if not workflows_dir.exists():
            self.logger.error("❌ Workflows folder not found!")
            return []
        
        available = []
        for workflow_type in self.all_workflows:
            workflow_file = workflows_dir / f"{workflow_type}.json"
            if workflow_file.exists():
                available.append(workflow_type)
                self.logger.debug(f"✅ Found: {workflow_type}.json")
            else:
                self.logger.debug(f"⚠️  Missing: {workflow_type}.json")
        
        self.available_workflows = available
        self.logger.info(f"📁 Available workflows: {len(available)}/{len(self.all_workflows)}")
        self.logger.info(f"🎯 Ready: {', '.join(available)}")
        
        if len(available) == 0:
            self.logger.error("❌ No workflow files found in workflows/ folder!")
        
        return available

    def parse_prompt_file(self, filepath: str) -> List[Dict[str, Any]]:
        """Parse prompt file with format [∆{workflow}•count∆ ¥positive¥ §§negative§§] or [∆{workflow}•count∆ ¥positive¥]"""
        prompts = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            # Split berdasarkan bracket
            tasks = re.findall(r'\[[^\]]+\]', content)
            
            for task_idx, task in enumerate(tasks, 1):
                try:
                    # Extract workflows dan counts
                    workflow_matches = re.findall(r'\{([^}]+)\}•(\d+)', task)
                    
                    # Extract positive dan negative prompt
                    prompt_match = re.search(r'¥([^¥]+)¥(?:\s*§§([^§]+)§§)?', task)
                    
                    if not workflow_matches or not prompt_match:
                        self.logger.warning(f"Task {task_idx}: Invalid format - {task[:50]}...")
                        continue
                    
                    positive_prompt = prompt_match.group(1).strip()
                    negative_prompt = prompt_match.group(2).strip() if prompt_match.group(2) else ""
                    
                    ratios = []
                    total_count = 0
                    
                    for workflow_name, count_str in workflow_matches:
                        workflow_name = workflow_name.strip()
                        count = int(count_str)
                        
                        if workflow_name in self.all_workflows and count > 0:
                            ratios.append({'type': workflow_name, 'count': count})
                            total_count += count
                        else:
                            self.logger.warning(f"Task {task_idx}: Invalid workflow '{workflow_name}' or count '{count}'")
                    
                    if ratios and total_count > 0:
                        prompts.append({
                            'positive_text': positive_prompt,
                            'negative_text': negative_prompt,
                            'ratios': ratios,
                            'task_num': task_idx,
                            'total_count': total_count
                        })
                        neg_info = f" + negative" if negative_prompt else ""
                        self.logger.debug(f"Task {task_idx}: {total_count} generations for '{positive_prompt[:30]}...'{neg_info}")
                
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

    def load_workflow(self, workflow_type: str) -> Optional[Dict[str, Any]]:
        """Load workflow with caching"""
        if workflow_type in self._workflow_cache:
            return self._workflow_cache[workflow_type].copy()
        
        workflow_path = Path('workflows') / f"{workflow_type}.json"
        if not workflow_path.exists():
            return None
        
        try:
            with open(workflow_path, 'r', encoding='utf-8') as f:
                workflow = json.load(f)
            
            self._workflow_cache[workflow_type] = workflow
            self.logger.debug(f"📥 Cached workflow: {workflow_type}")
            return workflow.copy()
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON in {workflow_type}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Error loading {workflow_type}: {e}")
            return None

    def update_workflow_prompt(self, workflow: Dict[str, Any], prompt_text: str) -> Dict[str, Any]:
        """Update workflow with new prompt and random seed"""
        updated_nodes = 0
        
        for node_id, node_data in workflow.items():
            if isinstance(node_data, dict):
                class_type = node_data.get('class_type')
                
                # Update prompt text
                if class_type == 'CLIPTextEncode':
                    inputs = node_data.get('inputs', {})
                    if 'text' in inputs:
                        inputs['text'] = prompt_text
                        updated_nodes += 1
                
                # Update seed for variation
                elif class_type in ['KSampler', 'KSamplerAdvanced']:
                    inputs = node_data.get('inputs', {})
                    if 'seed' in inputs:
                        inputs['seed'] = random.randint(1, 2**32 - 1)
        
        if updated_nodes == 0:
            self.logger.warning("⚠️  No CLIPTextEncode nodes found to update prompt")
        
        return workflow

    def queue_prompt_with_retry(self, workflow: Dict[str, Any], attempt: int = 1) -> Optional[str]:
        """Queue prompt with retry mechanism"""
        try:
            payload = {"prompt": workflow, "client_id": self.client_id}
            
            response = self.session.post(
                f"http://{self.server_address}/prompt",
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            if 'prompt_id' in result:
                return result['prompt_id']
            else:
                self.logger.error(f"❌ No prompt_id in response: {result}")
                return None
                
        except requests.exceptions.RequestException as e:
            if attempt <= self.retry_attempts:
                wait_time = 2 ** attempt
                self.logger.warning(f"⚠️  Queue attempt {attempt} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                self.stats['retries'] += 1
                return self.queue_prompt_with_retry(workflow, attempt + 1)
            else:
                self.logger.error(f"❌ Queue failed after {self.retry_attempts} attempts: {e}")
                return None

    def wait_for_completion(self, prompt_id: str, timeout: int = 600) -> bool:
        """Wait for completion with adaptive checking"""
        start_time = time.time()
        check_interval = 3
        max_interval = 15
        
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(
                    f"http://{self.server_address}/history/{prompt_id}",
                    timeout=10
                )
                
                if response.status_code == 200:
                    history = response.json()
                    if prompt_id in history:
                        return True
                elif response.status_code == 404:
                    # Check if still in queue
                    queue_response = self.session.get(f"http://{self.server_address}/queue")
                    if queue_response.status_code == 200:
                        queue_data = queue_response.json()
                        in_queue = any(
                            item[1]['prompt_id'] == prompt_id 
                            for item in queue_data.get('queue_running', []) + queue_data.get('queue_pending', [])
                        )
                        if not in_queue:
                            self.logger.error(f"❌ Prompt {prompt_id} not found in queue or history")
                            return False
                
                # Adaptive sleep
                elapsed = time.time() - start_time
                if elapsed > 120:  # After 2 minutes, increase interval
                    check_interval = min(max_interval, check_interval + 1)
                
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"❌ Error checking {prompt_id}: {e}")
                time.sleep(5)
        
        self.logger.error(f"⏰ Timeout waiting for {prompt_id} after {timeout}s")
        return False

    def process_single_generation(self, positive_prompt: str, negative_prompt: str, workflow_type: str, gen_index: str) -> Dict[str, Any]:
        """Process single generation"""
        job_id = f"{workflow_type}_{gen_index}_{int(time.time())}"
        
        # Check if workflow is available
        if workflow_type not in self.available_workflows:
            self.stats['skipped'] += 1
            self.logger.warning(f"⏭️  Skipped {job_id} - workflow not available")
            return {
                'status': 'skipped',
                'job_id': job_id,
                'workflow_type': workflow_type,
                'reason': 'workflow_not_available'
            }
        
        try:
            workflow = self.load_workflow(workflow_type)
            if not workflow:
                self.stats['skipped'] += 1
                return {
                    'status': 'skipped',
                    'job_id': job_id,
                    'workflow_type': workflow_type,
                    'reason': 'workflow_load_failed'
                }
            
            workflow = self.update_workflow_prompt(workflow, positive_prompt, negative_prompt)
            
            prompt_id = self.queue_prompt_with_retry(workflow)
            if not prompt_id:
                self.stats['failed'] += 1
                return {
                    'status': 'failed',
                    'job_id': job_id,
                    'workflow_type': workflow_type,
                    'error': 'queue_failed'
                }
            
            # Track active job
            with self.job_lock:
                self.active_jobs[prompt_id] = {
                    'job_id': job_id,
                    'workflow_type': workflow_type,
                    'start_time': time.time()
                }
            
            self.stats['total_queued'] += 1
            self.logger.info(f"🚀 Queued {job_id} (ID: {prompt_id})")
            
            if self.wait_for_completion(prompt_id):
                with self.job_lock:
                    if prompt_id in self.active_jobs:
                        duration = time.time() - self.active_jobs[prompt_id]['start_time']
                        del self.active_jobs[prompt_id]
                
                self.stats['completed'] += 1
                self.logger.info(f"✅ Completed {job_id} in {duration:.1f}s")
                return {
                    'status': 'completed',
                    'job_id': job_id,
                    'prompt_id': prompt_id,
                    'workflow_type': workflow_type,
                    'duration': duration
                }
            else:
                with self.job_lock:
                    if prompt_id in self.active_jobs:
                        del self.active_jobs[prompt_id]
                
                self.stats['failed'] += 1
                return {
                    'status': 'timeout',
                    'job_id': job_id,
                    'prompt_id': prompt_id,
                    'workflow_type': workflow_type
                }
                
        except Exception as e:
            self.stats['failed'] += 1
            self.logger.error(f"❌ Error in {job_id}: {e}")
            return {
                'status': 'error',
                'job_id': job_id,
                'error': str(e),
                'workflow_type': workflow_type
            }

    def process_prompts(self, prompt_file: str) -> bool:
        """Main processing function"""
        self.logger.info("🎯 ComfyUI Auto Batch Processor Started")
        
        # Test connection
        if not self.test_connection():
            self.logger.error("❌ Cannot connect to ComfyUI server!")
            return False
        
        # Scan available workflows
        available = self.scan_available_workflows()
        if not available:
            self.logger.error("❌ No workflows available!")
            return False
        
        # Parse prompts
        prompts = self.parse_prompt_file(prompt_file)
        if not prompts:
            self.logger.error("❌ No valid prompts found!")
            return False
        
        total_generations = sum(p['total_count'] for p in prompts)
        self.logger.info(f"🎯 Starting batch: {len(prompts)} tasks, {total_generations} generations")
        self.logger.info(f"⚙️  Max concurrent: {self.max_concurrent}")
        
        self.stats['start_time'] = time.time()
        
        # Prepare tasks
        tasks = []
        for prompt_data in prompts:
            positive_prompt = prompt_data['positive_text']
            negative_prompt = prompt_data['negative_text']
            task_num = prompt_data['task_num']
            
            for ratio_data in prompt_data['ratios']:
                workflow_type = ratio_data['type']
                count = ratio_data['count']
                
                for i in range(count):
                    tasks.append((positive_prompt, negative_prompt, workflow_type, f"T{task_num}_{workflow_type}_{i+1}"))
        
        # Execute with ThreadPoolExecutor
        results = []
        try:
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                future_to_task = {
                    executor.submit(self.process_single_generation, pos_text, neg_text, wf_type, idx): (pos_text, neg_text, wf_type, idx)
                    for pos_text, neg_text, wf_type, idx in tasks
                }
                
                for future in as_completed(future_to_task):
                    result = future.result()
                    results.append(result)
                    
                    # Progress update
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

    def print_final_stats(self):
        """Print comprehensive statistics"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("\n" + "="*60)
        self.logger.info("📈 BATCH PROCESSING COMPLETE")
        self.logger.info("="*60)
        self.logger.info(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f}min)")
        self.logger.info(f"🚀 Queued: {self.stats['total_queued']}")
        self.logger.info(f"✅ Completed: {self.stats['completed']}")
        self.logger.info(f"❌ Failed: {self.stats['failed']}")
        self.logger.info(f"⏭️  Skipped: {self.stats['skipped']}")
        self.logger.info(f"🔄 Retries: {self.stats['retries']}")
        
        if self.stats['completed'] > 0:
            avg_time = duration / self.stats['completed']
            self.logger.info(f"⚡ Avg per Generation: {avg_time:.1f}s")
        
        total_attempted = self.stats['total_queued']
        if total_attempted > 0:
            success_rate = (self.stats['completed'] / total_attempted) * 100
            self.logger.info(f"📊 Success Rate: {success_rate:.1f}%")
        
        self.logger.info("="*60)

    def test_connection(self) -> bool:
        """Test connection to ComfyUI server"""
        try:
            response = self.session.get(f"http://{self.server_address}/system_stats", timeout=10)
            if response.status_code == 200:
                queue_response = self.session.get(f"http://{self.server_address}/queue", timeout=5)
                if queue_response.status_code == 200:
                    self.logger.info("✅ ComfyUI connection successful")
                    return True
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ Cannot connect to ComfyUI at http://{self.server_address}")
        except requests.exceptions.Timeout:
            self.logger.error(f"❌ Connection timeout to ComfyUI")
        except Exception as e:
            self.logger.error(f"❌ Connection test failed: {e}")
        
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python batch_processor.py <prompt_file.txt> [max_concurrent] [server_address]")
        print("Example: python batch_processor.py prompts/my_prompts.txt 3 127.0.0.1:8188")
        sys.exit(1)
    
    prompt_file = sys.argv[1]
    max_concurrent = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    server_address = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:8188"
    
    if not os.path.exists(prompt_file):
        print(f"❌ File not found: {prompt_file}")
        sys.exit(1)
    
    processor = ComfyUIAutoBatch(
        server_address=server_address,
        max_concurrent=max_concurrent
    )
    
    try:
        success = processor.process_prompts(prompt_file)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)
