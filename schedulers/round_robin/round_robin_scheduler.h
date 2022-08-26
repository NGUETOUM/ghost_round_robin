/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 #ifndef GHOST_SCHEDULERS_ROUND_ROBIN_ROUND_ROBIN_SCHEDULER_H
 #define GHOST_SCHEDULERS_ROUND_ROBIN_ROUND_ROBIN_SCHEDULER_H


 #include <cstdint>
 #include <map>

 #include "absl/container/flat_hash_map.h"
 #include "absl/functional/bind_front.h"
 #include "absl/time/time.h"
 #include "lib/agent.h"
 #include "lib/scheduler.h"
 //#include "schedulers/shinjuku/shinjuku_orchestrator.h"
 //#include "shared/prio_table.h"

 namespace ghost {

   // Store information about a scheduled task.
   struct RoundRobinTask : public Task<> {

     enum class RunState {
       kBlocked,
       kQueued,
       kOnCpu,
       kYielding,
       kPaused,
     };


     explicit RoundRobinTask(Gtid round_robin_task_gtid, struct ghost_sw_info sw_info)
         : Task<>(round_robin_task_gtid, sw_info) {}
     ~RoundRobinTask() override {}

     bool paused() const { return run_state == RunState::kPaused; }
     bool blocked() const { return run_state == RunState::kBlocked; }
     bool queued() const { return run_state == RunState::kQueued; }
     bool oncpu() const { return run_state == RunState::kOnCpu; }
     bool yielding() const { return run_state == RunState::kYielding; }

     // Sets the task's runtime to 'runtime' and updates the elapsed runtime if
     // 'update_elapsed_runtime' is true.
     void SetRuntime(absl::Duration runtime, bool update_elapsed_runtime);
     // Updates the task's runtime to match what the kernel last stored in the
     // task's status word. Note that this method does not use the syscall to get
     // the task's current runtime.
     void UpdateRuntime();

     static std::string_view RunStateToString(RoundRobinTask::RunState run_state) {
       switch (run_state) {
         case RoundRobinTask::RunState::kBlocked:
           return "Blocked";
         case RoundRobinTask::RunState::kQueued:
           return "Queued";
         case RoundRobinTask::RunState::kOnCpu:
           return "OnCpu";
         case RoundRobinTask::RunState::kYielding:
           return "Yielding";
         case RoundRobinTask::RunState::kPaused:
           return "Paused";
           // We will get a compile error if a new member is added to the
           // `RoundRobinTask::RunState` enum and a corresponding case is not added
           // here.
       }
       CHECK(false);
       return "Unknown run state";
     }

     friend std::ostream& operator<<(std::ostream& os,
                                     RoundRobinTask::RunState run_state) {
       os << RunStateToString(run_state);
       return os;
     }


     RunState run_state = RunState::kBlocked;
     int cpu = -1;

     // Priority boosting for jumping past regular round robin ordering in the
     // runqueue.
     //
     // A task's priority is boosted on a kernel preemption or a !deferrable
     // wakeup - basically when it may be holding locks or other resources
     // that prevent other tasks from making progress.
     bool prio_boost = false;

     // Cumulative runtime in ns.
     absl::Duration runtime;
     // Accrued CPU time in ns.
     absl::Duration elapsed_runtime;
     // The time that the task was last scheduled.
     absl::Time last_ran = absl::UnixEpoch();

     // Whether the last execution was preempted or not.
     bool preempted = false;

     //std::shared_ptr<ShinjukuOrchestrator> orch;
     //const ShinjukuSchedParams* sp = nullptr;
     uint32_t _qos = 0;
     bool has_work = false;
     //uint32_t wcid = std::numeric_limits<uint32_t>::max();

     // Indicates whether there is a pending deferred unschedule for this task, and
     // if so, whether the unschedule could optionally happen or must happen.
     //UnscheduleLevel unschedule_level = UnscheduleLevel::kNoUnschedule;
   };


   // Implements the global agent policy layer and the Round Robin scheduling
   // algorithm.
   class RoundRobinScheduler : public BasicDispatchScheduler<RoundRobinTask> {
    public:
     explicit RoundRobinScheduler(
         Enclave* enclave, CpuList cpulist,
         std::shared_ptr<TaskAllocator<RoundRobinTask>> allocator,
         int32_t global_cpu, absl::Duration preemption_time_slice);
     ~RoundRobinScheduler() final;

     void EnclaveReady() final;
     Channel& GetDefaultChannel() final { return global_channel_; };

     // Handles task messages received from the kernel via shared memory queues.
     void TaskNew(RoundRobinTask* task, const Message& msg) final;
     void TaskRunnable(RoundRobinTask* task, const Message& msg) final;
     void TaskDeparted(RoundRobinTask* task, const Message& msg) final;
     void TaskDead(RoundRobinTask* task, const Message& msg) final;
     void TaskYield(RoundRobinTask* task, const Message& msg) final;
     void TaskBlocked(RoundRobinTask* task, const Message& msg) final;
     void TaskPreempted(RoundRobinTask* task, const Message& msg) final;

     void DiscoveryStart() final;
     void DiscoveryComplete() final;

     bool Empty() { return num_tasks_ == 0; }

     // Refreshes updated sched items. Note that all sched items may be refreshed,
     // regardless of whether they have been updated or not, if the stream has
     // overflown.
     //void UpdateSchedParams();

     // Removes 'task' from the runqueue.
     void RemoveFromRunqueue(RoundRobinTask* task);

     // Helper function to 'GlobalSchedule' that determines whether it should skip
     // scheduling a CPU right now (returns 'true') or if it can schedule a CPU
     // right now (returns 'false').
     bool SkipForSchedule(int iteration, const Cpu& cpu);

     // Main scheduling function for the global agent.
     void GlobalSchedule(const StatusWord& agent_sw,
                         StatusWord::BarrierToken agent_sw_last);

     int32_t GetGlobalCPUId() {
       return global_cpu_.load(std::memory_order_acquire);
     }

     void SetGlobalCPU(const Cpu& cpu) {
       global_cpu_.store(cpu.id(), std::memory_order_release);
     }

     // When a different scheduling class (e.g., CFS) has a task to run on the
     // global agent's CPU, the global agent calls this function to try to pick a
     // new CPU to move to and, if a new CPU is found, to initiate the handoff
     // process.
     void PickNextGlobalCPU(StatusWord::BarrierToken agent_barrier);

     // Print debug details about the current tasks managed by the global agent,
     // CPU state, and runqueue stats.
     void DumpState(const Cpu& cpu, int flags) final;
     std::atomic<bool> debug_runqueue_ = false;

     static constexpr int kDebugRunqueue = 1;

     //std::vector<std::pair<int,ShinjukuTask*>> tasks_table;
     //ShinjukuTask* findElement(uint32_t sid);


    private:
     struct CpuState {
       RoundRobinTask* current = nullptr;
       RoundRobinTask* next = nullptr;
       const Agent* agent = nullptr;
     } ABSL_CACHELINE_ALIGNED;

     // Stop 'task' from running and schedule nothing in its place. 'task' must be
     // currently running on a CPU.
     void UnscheduleTask(RoundRobinTask* task);

     // Marks a task as yielded.
     void Yield(RoundRobinTask* task);

     // Unmarks a task as yielded.
     void Unyield(RoundRobinTask* task);

     // Adds a task to the FIFO runqueue. By default, the task is added to the back
     // of the FIFO ('back' == true), but will be added to the front of the FIFO if
     // 'back' == false or if 'task->prio_boost' == true. When 'task->prio_boost'
     // == true, the task was unexpectedly preempted (e.g., by CFS) and could be
     // holding a critical lock, so we want to schedule it again as soon as
     // possible so it can release the lock. This could improve performance.
     void Enqueue(RoundRobinTask* task, bool back = true);

     // Removes and returns the task at the front of the runqueue.
     RoundRobinTask* Dequeue();

     // Returns (but does not remove) the task at the front of the runqueue.
     RoundRobinTask* Peek();

     // Prints all tasks (includin tasks not running or on the runqueue) managed by
     // the global agent.
     void DumpAllTasks();

     // Updates the task's runtime and performs some consistency checks.
     void UpdateTaskRuntime(RoundRobinTask* task, absl::Duration new_runtime,
                            bool update_elapsed_runtime);

     // Callback when a sched item is updated.
     //void SchedParamsCallback(ShinjukuOrchestrator& orch, const ShinjukuSchedParams* sp, Gtid oldgtid);

     // Handles a new process that has at least one of its threads enter the ghOSt
     // scheduling class (e.g., via sched_setscheduler()).
     //void HandleNewGtid(ShinjukuTask* task, pid_t tgid);

     // Returns 'true' if a CPU can be scheduled by ghOSt. Returns 'false'
     // otherwise, usually because a higher-priority scheduling class (e.g., CFS)
     // is currently using the CPU.
     bool Available(const Cpu& cpu);

     CpuState* cpu_state_of(const RoundRobinTask* task);

     CpuState* cpu_state(const Cpu& cpu) { return &cpu_states_[cpu.id()]; }

     size_t RunqueueSize() const {
       size_t size = 0;
       for (const auto& [qos, rq] : run_queue_) {
         size += rq.size();
       }
       return size;
     }

     bool RunqueueEmpty() const { return RunqueueSize() == 0; }

     // Returns the highest-QoS runqueue that has at least one task enqueued.
     // Must call this on a non-empty runqueue.
     uint32_t FirstFilledRunqueue() const {
       for (auto it = run_queue_.rbegin(); it != run_queue_.rend(); it++) {
         if (!it->second.empty()) {
           return it->first;
         }
       }
       // A precondition for this method is that the runqueue must be non-empty, so
       // if we get down here, that precondition was not upheld.
       CHECK(false);
       // Return something so that the compiler doesn't complain, but we will never
       // execute this return statement due to the 'CHECK(false)' above.
       return std::numeric_limits<uint32_t>::max();
     }

     CpuState cpu_states_[MAX_CPUS];

     std::atomic<int32_t> global_cpu_;
     LocalChannel global_channel_;
     int num_tasks_ = 0;
     //int num_sid_ = 0;
     bool in_discovery_ = false;

     // Map from QoS level to runqueue
     // We use an 'std::map' rather than 'absl::flat_hash_map' because we need to
     // iterate on the map in order of QoS level.
     std::map<uint32_t, std::deque<RoundRobinTask*>> run_queue_;
     std::vector<RoundRobinTask*> paused_repeatables_;
     std::vector<RoundRobinTask*> yielding_tasks_;
     //absl::flat_hash_map<pid_t, std::shared_ptr<ShinjukuOrchestrator>> orchs_;
     /*const ShinjukuOrchestrator::SchedCallbackFunc kSchedCallbackFunc =
         absl::bind_front(&ShinjukuScheduler::SchedParamsCallback, this);*/
     const absl::Duration preemption_time_slice_;
   };

   // Initializes the task allocator and the RoundRobin scheduler.
   std::unique_ptr<RoundRobinScheduler> SingleThreadRoundRobinScheduler(
       Enclave* enclave, CpuList cpulist, int32_t global_cpu,
       absl::Duration preemption_time_slice);

   // Operates as the Global or Satellite agent depending on input from the
   // global_scheduler->GetGlobalCPU callback.
   class RoundRobinAgent : public LocalAgent {
    public:
     RoundRobinAgent(Enclave* enclave, Cpu cpu, RoundRobinScheduler* global_scheduler)
         : LocalAgent(enclave, cpu), global_scheduler_(global_scheduler) {}

     void AgentThread() override;
     Scheduler* AgentScheduler() const override { return global_scheduler_; }

    private:
     RoundRobinScheduler* global_scheduler_;
   };

   class RoundRobinConfig : public AgentConfig {
    public:
     RoundRobinConfig() {}
     RoundRobinConfig(Topology* topology, CpuList cpulist, Cpu global_cpu)
         : AgentConfig(topology, std::move(cpulist)), global_cpu_(global_cpu) {}

     Cpu global_cpu_{Cpu::UninitializedType::kUninitialized};
     absl::Duration preemption_time_slice_;
   };

   // An global agent scheduler.  It runs a single-threaded RoundRobin scheduler on
   // the global_cpu.
   template <class EnclaveType>
   class FullRoundRobinAgent : public FullAgent<EnclaveType> {
    public:
     explicit FullRoundRobinAgent(RoundRobinConfig config)
         : FullAgent<EnclaveType>(config) {
       global_scheduler_ = SingleThreadRoundRobinScheduler(
           &this->enclave_, *this->enclave_.cpus(), config.global_cpu_.id(),
           config.preemption_time_slice_);
       this->StartAgentTasks();
       this->enclave_.Ready();
     }

     ~FullRoundRobinAgent() override {
       // Terminate global agent before satellites to avoid a false negative error
       // from ghost_run(). e.g. when the global agent tries to schedule on a CPU
       // without an active satellite agent.
       int global_cpuid = global_scheduler_->GetGlobalCPUId();

       if (this->agents_.front()->cpu().id() != global_cpuid) {
         // Bring the current globalcpu agent to the front.
         for (auto it = this->agents_.begin(); it != this->agents_.end(); it++) {
           if (((*it)->cpu().id() == global_cpuid)) {
             auto d = std::distance(this->agents_.begin(), it);
             std::iter_swap(this->agents_.begin(), this->agents_.begin() + d);
             break;
           }
         }
       }

       CHECK_EQ(this->agents_.front()->cpu().id(), global_cpuid);

       this->TerminateAgentTasks();
     }

     std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) override {
       return absl::make_unique<RoundRobinAgent>(&this->enclave_, cpu,
                                               global_scheduler_.get());
     }

     void RpcHandler(int64_t req, const AgentRpcArgs& args,
                     AgentRpcResponse& response) override {
       switch (req) {
         case RoundRobinScheduler::kDebugRunqueue:
           global_scheduler_->debug_runqueue_ = true;
           response.response_code = 0;
           return;
         default:
           response.response_code = -1;
           return;
       }
     }

    private:
     std::unique_ptr<RoundRobinScheduler> global_scheduler_;
   };

 }


#endif
