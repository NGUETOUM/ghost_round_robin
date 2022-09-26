// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "schedulers/python_round_robin/round_robin_scheduler.h"
#include <pthread.h>
#include "absl/strings/str_format.h"

namespace ghost {

void RoundRobinTask::SetRuntime(absl::Duration new_runtime,
                              bool update_elapsed_runtime) {
  CHECK_GE(new_runtime, runtime);
  if (update_elapsed_runtime) {
    elapsed_runtime += new_runtime - runtime;
  }
  runtime = new_runtime;
}

void RoundRobinTask::UpdateRuntime() {
  // We read the runtime from the status word rather than make a syscall. The
  // runtime in the status word may be out-of-sync with the true runtime by up
  // to 10 microseconds (which is the timer interrupt period). This is alright
  // for Shinjuku since we only need an accurate measure of the task's runtime
  // to implement the preemption time slice and the global agent uses Abseil's
  // clock functionality to approximate the current elapsed runtime. The syscall
  // will return the true runtime for the task, but it needs to acquire a
  // runqueue lock if the task is currently running which harms tail latency.
  absl::Duration runtime = absl::Nanoseconds(status_word.runtime());
  SetRuntime(runtime, /* update_elapsed_runtime = */ true);
}

RoundRobinScheduler::RoundRobinScheduler(
    Enclave* enclave, CpuList cpulist,
    std::shared_ptr<TaskAllocator<RoundRobinTask>> allocator, int32_t global_cpu/*,
    absl::Duration preemption_time_slice*/)
    : BasicDispatchScheduler(enclave, std::move(cpulist), std::move(allocator)),
      global_cpu_(global_cpu),
      global_channel_(GHOST_MAX_QUEUE_ELEMS, /*node=*/0)
      /*preemption_time_slice_(preemption_time_slice)*/ {
  if(!cpus().IsSet(global_cpu_)) {
    Cpu c = cpus().Front();
    CHECK(c.valid());
    global_cpu_ = c.id();
  }
}

RoundRobinScheduler::~RoundRobinScheduler() {}

void RoundRobinScheduler::EnclaveReady() {
  for (const Cpu& cpu : cpus()) {
    CpuState* cs = cpu_state(cpu);
    cs->agent = enclave()->GetAgent(cpu);
    CHECK_NE(cs->agent, nullptr);
  }
}

bool RoundRobinScheduler::Available(const Cpu& cpu) {
  CpuState* cs = cpu_state(cpu);
  return cs->agent->cpu_avail();
}

void RoundRobinScheduler::DumpAllTasks() {
  fprintf(stderr, "task                     state       cpu  P\n");
  allocator()->ForEachTask([](Gtid gtid, const RoundRobinTask* task) {
    absl::FPrintF(stderr, "%-12s                 %-12s       %-2d    %c\n", gtid.describe(),
                  RoundRobinTask::RunStateToString(task->run_state), task->cpu,
                  task->prio_boost ? 'P' : '-');
    return true;
  });
}

void RoundRobinScheduler::DumpState(const Cpu& agent_cpu, int flags) {
  if (flags & kDumpAllTasks) {
    DumpAllTasks();
  }

  if (!(flags & kDumpStateEmptyRQ) && RunqueueEmpty()) {
    return;
  }

  fprintf(stderr, "SchedState: ");
  for (const Cpu& cpu : cpus()) {
    CpuState* cs = cpu_state(cpu);
    fprintf(stderr, "%d:", cpu.id());
    if (!cs->current) {
      fprintf(stderr, "none ");
    } else {
      Gtid gtid = cs->current->gtid;
      absl::FPrintF(stderr, "%s ", gtid.describe());
    }
  }
  fprintf(stderr, " rq_l=%ld", RunqueueSize());
  fprintf(stderr, "\n");
}

RoundRobinScheduler::CpuState* RoundRobinScheduler::cpu_state_of(
    const RoundRobinTask* task) {
  CHECK(task->oncpu());
  CpuState* result = &cpu_states_[task->cpu];
  CHECK_EQ(result->current, task);
  return result;
}

void RoundRobinScheduler::UpdateTaskRuntime(RoundRobinTask* task,
                                          absl::Duration new_runtime,
                                          bool update_elapsed_runtime) {
  task->SetRuntime(new_runtime, update_elapsed_runtime);
}

/*void getThreadPriority(Gtid gtid){
pthread_t tid;
sched_param param;
int priority;
int policy = 18;
int ret;

tid = static_cast<pthread_t>(gtid.id());

// scheduling parameters of target thread
//ret = pthread_getschedparam(tid, &policy, &param);
printf("\n Thread id in getPriority Function is %ld  pthread_t = %ld \n", gtid.id(), tid);

 fprintf(stderr, "\n pthread_getschedparam(tid, &policy, &param) failed: %d\n", pthread_getschedparam(tid, &policy, &param), strerror(errno));

printf("\n Thread id in getPriority Function is %d \n", gtid.tid());
// sched_priority contains the priority of the thread
priority = param.sched_priority;

printf("thread with tid %d have priority equal to %d ", gtid.tid(), priority);

}*/

void RoundRobinScheduler::TaskNew(RoundRobinTask* task, const Message& msg) {
  const ghost_msg_payload_task_new* payload =
      static_cast<const ghost_msg_payload_task_new*>(msg.payload());

  DCHECK_EQ(payload->runtime, task->status_word.runtime());

  UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                    /* update_elapsed_runtime = */ false);
  task->seqnum = msg.seqnum();
  task->run_state = RoundRobinTask::RunState::kBlocked;  // Need this in the
                                                       // runnable case anyway.

  task->has_work = true;
  //task->_qos = num_tasks_ + 1;
  const Gtid gtid(payload->gtid);
  printf("\n %d-  Welcome %d \n", num_tasks_, gtid.tid());

  //task->_qos = num_tasks_;

  //getThreadPriority(msg.gtid());
  if (payload->runnable) Enqueue(task);

  num_tasks_++;

}

void RoundRobinScheduler::TaskRunnable(RoundRobinTask* task, const Message& msg) {
  const ghost_msg_payload_task_wakeup* payload =
      static_cast<const ghost_msg_payload_task_wakeup*>(msg.payload());

  CHECK(task->blocked());

  // A non-deferrable wakeup gets the same preference as a preempted task.
  // This is because it may be holding locks or resources needed by other
  // tasks to make progress.
  task->prio_boost = !payload->deferrable;

  Enqueue(task, /* back = */ false);
  printf("\n Task %d is Runnable\n", task->gtid.tid());
  auto it = std::find(blocked_tasks_.begin(), blocked_tasks_.end(), task);
  CHECK(it != blocked_tasks_.end());
  blocked_tasks_.erase(it);
}

void RoundRobinScheduler::TaskDeparted(RoundRobinTask* task, const Message& msg) {
  if (task->oncpu()) {
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
  } /*else if (task->queued()) {
    RemoveFromRunqueue(task);
  }*/ else {
    CHECK(task->blocked());
  }

  allocator()->FreeTask(task);
  num_tasks_--;
}

void RoundRobinScheduler::TaskDead(RoundRobinTask* task, const Message& msg) {
  CHECK_EQ(task->run_state, RoundRobinTask::RunState::kBlocked);
  allocator()->FreeTask(task);
  num_tasks_--;
}

void RoundRobinScheduler::TaskBlocked(RoundRobinTask* task, const Message& msg) {
  const ghost_msg_payload_task_blocked* payload =
      reinterpret_cast<const ghost_msg_payload_task_blocked*>(msg.payload());

  DCHECK_EQ(payload->runtime, task->status_word.runtime());

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // blocking (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
  } /*else if (task->queued()) {
    RemoveFromRunqueue(task);
  }*/ else {
    CHECK(task->paused());
  }
  task->run_state = RoundRobinTask::RunState::kBlocked;
  blocked_tasks_.emplace_back(task);
  printf("\n Task %d is Blocked\n", task->gtid.tid());
}

void RoundRobinScheduler::TaskPreempted(RoundRobinTask* task, const Message& msg) {
  const ghost_msg_payload_task_preempt* payload =
      reinterpret_cast<const ghost_msg_payload_task_preempt*>(msg.payload());

  DCHECK_EQ(payload->runtime, task->status_word.runtime());

  task->preempted = true;
  //A regarder lors des tests
  task->prio_boost = false;
  printf("\n Task %d is Preempted\n", task->gtid.tid());
  // States other than the typical kOnCpu are possible here:
  // We could be kQueued from a TASK_NEW that was immediately preempted.
  // We could be kPaused if agent-initiated preemption raced with kernel
  // preemption (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
    Enqueue(task);
  } /*else if (task->queued()) {
    // The task was preempted, so add it to the front of run queue. We do this
    // because (1) the task could have been holding an important lock when it
    // was preempted so we could improve performance by scheduling the task
    // again as soon as possible and (2) because the Shinjuku algorithm assumes
    // tasks are not preempted by other scheduling classes so getting the task
    // scheduled back onto the CPU as soon as possible is important to
    // faithfully implement the algorithm.
    //RemoveFromRunqueue(task);
    //Enqueue(task);
  } else {
    CHECK(task->paused());
  }*/

  /*if (!RunqueueEmpty()) {
    printf("\n Run_queue Size = %ld \n", run_queue_.size());
    printf("\n yielding_queue Size = %ld \n", yielding_tasks_.size());
    printf("\n Blocked_queue Size = %ld \n", blocked_tasks_.size());
  }*/
}

void RoundRobinScheduler::TaskYield(RoundRobinTask* task, const Message& msg) {
  const ghost_msg_payload_task_yield* payload =
      reinterpret_cast<const ghost_msg_payload_task_yield*>(msg.payload());

  DCHECK_EQ(payload->runtime, task->status_word.runtime());

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // yielding (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  printf("\n Task %d is Yielded\n", task->gtid.tid());
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
    Yield(task);
  } else {
    CHECK(task->queued() || task->paused());
  }
}

/*void RoundRobinScheduler::DiscoveryStart() { in_discovery_ = true; }

void RoundRobinScheduler::DiscoveryComplete() {
  in_discovery_ = false;
}*/

void RoundRobinScheduler::Yield(RoundRobinTask* task) {
  // An oncpu() task can do a sched_yield() and get here via
  // ShinjukuTaskYield(). We may also get here if the scheduler wants to inhibit
  // a task from being picked in the current scheduling round (see
  // GlobalSchedule()).
  CHECK(task->oncpu() || task->queued());
  task->run_state = RoundRobinTask::RunState::kYielding;
  yielding_tasks_.emplace_back(task);
}

void RoundRobinScheduler::Unyield(RoundRobinTask* task) {
  CHECK(task->yielding());

  auto it = std::find(yielding_tasks_.begin(), yielding_tasks_.end(), task);
  CHECK(it != yielding_tasks_.end());
  yielding_tasks_.erase(it);

  Enqueue(task);
}

void RoundRobinScheduler::Enqueue(RoundRobinTask* task, bool back) {

  if (!task->has_work) {
    // We'll re-enqueue when this RoundRobinTask has work to do during periodic
    // scraping of PrioTable.
    task->run_state = RoundRobinTask::RunState::kPaused;
    return;
  }

  task->run_state = RoundRobinTask::RunState::kQueued;
  if(task->prio_boost){
    run_queue_.emplace_front(task);
  }else{
    run_queue_.emplace_back(task);
  }

  /*if (back && !task->prio_boost) {
    run_queue_[task->_qos].emplace_back(task);
  } else {
    run_queue_[task->_qos].emplace_front(task);
  }*/
}

RoundRobinTask* RoundRobinScheduler::Dequeue() {
  if (RunqueueEmpty()) {
    return nullptr;
  }

  //std::deque<RoundRobinTask*>& rq = run_queue_[FirstFilledRunqueue()];
  struct RoundRobinTask* task = run_queue_.front();
  CHECK_NE(task, nullptr);
  CHECK(task->has_work);
  run_queue_.pop_front();
  //rq.pop_front();

  return task;
}

/*RoundRobinTask* RoundRobinScheduler::Peek() {
  if (RunqueueEmpty()) {
    return nullptr;
  }

  RoundRobinTask* task = run_queue_[FirstFilledRunqueue()].front();
  CHECK(task->has_work);
  return task;
}*/

/*void RoundRobinScheduler::RemoveFromRunqueue(RoundRobinTask* task) {
  CHECK(task->queued());

  for (auto& [qos, rq] : run_queue_) {
    for (int pos = rq.size() - 1; pos >= 0; pos--) {
      // The [] operator for 'std::deque' is constant time
      if (rq[pos] == task) {
        task->run_state = RoundRobinTask::RunState::kPaused;
        rq.erase(rq.cbegin() + pos);
        return;
      }
    }
  }
  // This state is unreachable because the task is queued
  CHECK(false);
}*/

void RoundRobinScheduler::UnscheduleTask(RoundRobinTask* task) {
  CHECK_NE(task, nullptr);
  CHECK(task->oncpu());

  // Preempt `cpu` while ensuring that the transaction is committed on
  // the uber agent CPU (see `COMMIT_AT_TXN_COMMIT` below).
  Cpu cpu = topology()->cpu(task->cpu);
  RunRequest* req = enclave()->GetRunRequest(cpu);
  req->Open({
      .target = Gtid(0),  // redundant but emphasize that this is a preemption.
      .commit_flags = COMMIT_AT_TXN_COMMIT,
  });
  CHECK(req->Commit());

  CpuState* cs = cpu_state(cpu);
  cs->current = nullptr;
  task->run_state = RoundRobinTask::RunState::kOnCpu;
}


bool RoundRobinScheduler::SkipForSchedule(const Cpu& cpu) {
  CpuState* cs = cpu_state(cpu);
  // The logic is complex, so we break it into multiple if statements rather
  // than compress it into a single boolean expression that we return
  if (!Available(cpu) || cpu.id() == GetGlobalCPUId()) {
    // Cannot schedule on this CPU.
    return true;
  }

  if(cs->current || cs->next){
    return true;
  }

  return false;
}

void RoundRobinScheduler::GlobalSchedule(const StatusWord& agent_sw,
                                       StatusWord::BarrierToken agent_sw_last) {
  // List of CPUs with open transactions.
  CpuList open_cpus = MachineTopology()->EmptyCpuList();
  CpuList updated_cpus = MachineTopology()->EmptyCpuList();
  //RoundRobinTask* to_run = nullptr;
  //const absl::Time now = absl::Now();
  // TODO: Refactor this loop
  //for (int i = 0; i < 2; i++) {

    for (const Cpu& cpu : cpus()) {
      CpuState* cs = cpu_state(cpu);
      if (!SkipForSchedule(cpu)) {
        RoundRobinTask* to_run = Dequeue();
        //CHECK_NE(to_run, nullptr);
        cs->next = to_run;
        updated_cpus.Set(cpu.id());
      }

    }

    for (const Cpu& cpu : updated_cpus) {
      CpuState* cs = cpu_state(cpu);

      RoundRobinTask* next = cs->next;
      //CHECK_NE(next, nullptr);

      if (cs->current == next) continue;

      RunRequest* req = enclave()->GetRunRequest(cpu);
      req->Open({
          .target = next->gtid,
          .target_barrier = next->seqnum,
          .commit_flags = COMMIT_AT_TXN_COMMIT,
      });

      open_cpus.Set(cpu.id());
    }

  if (!open_cpus.Empty()) {
    enclave()->CommitRunRequests(open_cpus);
  }

  for (const Cpu& cpu : open_cpus) {
    CpuState* cs = cpu_state(cpu);
    RoundRobinTask* next = cs->next;
    cs->next = nullptr;

    RunRequest* req = enclave()->GetRunRequest(cpu);
    DCHECK(req->committed());
    if (req->state() == GHOST_TXN_COMPLETE) {
      if (cs->current) {
        RoundRobinTask* prev = cs->current;
        CHECK(prev->oncpu());

        // Update runtime of the preempted task.
        prev->UpdateRuntime();

        // Enqueue the preempted task so it is eligible to be picked again.
        prev->prio_boost = false;
        Enqueue(prev);
      }

      // ShinjukuTask latched successfully; clear state from an earlier run.
      //
      // Note that 'preempted' influences a task's run_queue position
      // so we clear it only after the transaction commit is successful.
      cs->current = next;
      next->run_state = RoundRobinTask::RunState::kOnCpu;
      next->cpu = cpu.id();
      next->preempted = false;
      next->prio_boost = false;
      // Clear the elapsed runtime so that the preemption timer is reset for
      // this task
      next->elapsed_runtime = absl::ZeroDuration();
      next->last_ran = absl::Now();
    } else {
      // Need to requeue in the stale case.
      Enqueue(next, /* back = */ false);
    }
  }

  // Yielding tasks are moved back to the runqueue having skipped one round
  // of scheduling decisions.
  if (!yielding_tasks_.empty()) {
    for (RoundRobinTask* t : yielding_tasks_) {
      CHECK_EQ(t->run_state, RoundRobinTask::RunState::kYielding);
      t->prio_boost = false;
      Enqueue(t);
    }
    yielding_tasks_.clear();
  }
}

void RoundRobinScheduler::PickNextGlobalCPU(
    StatusWord::BarrierToken agent_barrier) {
  // TODO: Select CPUs more intelligently.
  for (const Cpu& cpu : cpus()) {
    if (Available(cpu) && cpu.id() != GetGlobalCPUId()) {
      CpuState* cs = cpu_state(cpu);
      RoundRobinTask* prev = cs->current;

      if (prev) {
        CHECK(prev->oncpu());

        UnscheduleTask(prev);
        Yield(prev);
        return;

        // We ping the agent on `target` below. Once that agent wakes up, it
        // automatically preempts `prev`. The kernel generates a TASK_PREEMPT
        // message for `prev`, which allows the scheduler to update the state
        // for `prev`.
        //
        // This also allows the scheduler to gracefully handle the case where
        // `prev` actually blocks/yields/etc. before it is preempted by the
        // agent on `target`. In any of those cases, a
        // TASK_BLOCKED/TASK_YIELD/etc. message is delivered for `prev` instead
        // of a TASK_PREEMPT, so the state is still updated correctly for `prev`
        // even if it is not preempted by the agent.
      }

      SetGlobalCPU(cpu);
      enclave()->GetAgent(cpu)->Ping();
      break;
    }
  }
}

std::unique_ptr<RoundRobinScheduler> SingleThreadRoundRobinScheduler(
    Enclave* enclave, CpuList cpulist, int32_t global_cpu/*,
    absl::Duration preemption_time_slice*/) {
  auto allocator =
      std::make_shared<SingleThreadMallocTaskAllocator<RoundRobinTask>>();
  auto scheduler = absl::make_unique<RoundRobinScheduler>(
      enclave, std::move(cpulist), std::move(allocator), global_cpu/*,
      preemption_time_slice*/);
  return scheduler;
}

void RoundRobinAgent::AgentThread() {
  Channel& global_channel = global_scheduler_->GetDefaultChannel();
  gtid().assign_name("Agent:" + std::to_string(cpu().id()));
  if (verbose() > 1) {
    printf("Agent tid:=%d\n", gtid().tid());
  }
  SignalReady();
  WaitForEnclaveReady();

  PeriodicEdge debug_out(absl::Seconds(1));

  while (!Finished()) {
    StatusWord::BarrierToken agent_barrier = status_word().barrier();
    // Check if we're assigned as the Global agent.
    if (cpu().id() != global_scheduler_->GetGlobalCPUId()) {
      RunRequest* req = enclave()->GetRunRequest(cpu());

      /*if (verbose() > 1) {
        printf("Agent on cpu: %d Idled.\n", cpu().id());
      }*/
      req->LocalYield(agent_barrier, /*flags=*/0);
    } else {
      if (boosted_priority()) {
        global_scheduler_->PickNextGlobalCPU(agent_barrier);
        continue;
      }

      Message msg;
      while (!(msg = global_channel.Peek()).empty()) {
        global_scheduler_->DispatchMessage(msg);
        global_channel.Consume(msg);
      }

      // Order matters here: when a worker is PAUSED we defer the
      // preemption until GlobalSchedule() hoping to combine the
      // preemption with a remote_run.
      //
      // To restrict the visibility of this awkward state (PAUSED
      // but on_cpu) we do this immediately before GlobalSchedule().
      //global_scheduler_->UpdateSchedParams();

      global_scheduler_->GlobalSchedule(status_word(), agent_barrier);

      if (verbose() && debug_out.Edge()) {
        static const int flags =
            verbose() > 1 ? Scheduler::kDumpStateEmptyRQ : 0;
        global_scheduler_->debug_runqueue_ = true;
        if (global_scheduler_->debug_runqueue_) {
          global_scheduler_->debug_runqueue_ = false;
          global_scheduler_->DumpState(cpu(), Scheduler::kDumpAllTasks);
        } else {
          global_scheduler_->DumpState(cpu(), flags);
        }
      }
    }
  }
}

}  //  namespace ghost
