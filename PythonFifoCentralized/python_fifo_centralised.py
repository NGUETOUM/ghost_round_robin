print("Python is starting.")

from libpython_module import *
print("Importing bindings: OK")

import queue

print("Topology:", ", ".join((str([s.id() for s in c.siblings().ToVector()]) for c in ghost.GetTopoCpuList().ToVector())))

nurse = []

# Compare with fifo per_cpu C++ scheduler for comments

kBlocked = 0
kQueued = 1
kRunnable = 2
kOnCpu = 3
kYielding = 4


def prefix(x):
    return x*" " + str(x) + (8-x)*" "

class TaskData:
    def __init__(self):
        self.run_state = kBlocked
        self.cpu = -1
        self.preempted = False
        self.prio_boost = False

def attach_data(task):
    if type(task.pydata) is not TaskData:
        task.pydata = TaskData()

class CpuState:
    def __init__(self):
        self.current = None
        self.agent = None



class PyScheduler(ghost.BasicDispatchScheduler_PyTask_):
    def __init__(self, enclave, cpulist, global_cpu):
        self.allocator_ = ghost.SingleThreadMallocTaskAllocator_PyTask_()
        ghost.BasicDispatchScheduler_PyTask_.__init__(self, enclave, cpulist, self.allocator_)
        node = 0
        self.default_channel_ = ghost.LocalChannel(ghost.GHOST_MAX_QUEUE_ELEMS, node, ghost.GetEmptyCpuList())
        self.global_cpu = global_cpu
        self.run_queue = queue.deque()
        self.blocked_queue = queue.deque()
        self.num_tasks_ = 0
        self.in_discovery_ = False
        self.yielding_tasks_ = queue.deque()
        self.cpu_states = [None for _ in range(len(self.cpus().ToVector())+1)]
        if not self.cpus().IsSet(global_cpu):
            if self.cpus().Front().isValid():
                self.global_cpu = self.cpus().Front().id()

        for cpu in self.cpus().ToVector():
            self.cpu_states[cpu.id()] = CpuState()

    def cpu_state_of(self, task):
        return self.cpu_states[task.pydata.cpu]

    def Schedule(self):
        pass

    def EnclaveReady(self):
        check = False
        for cpu in self.cpus().ToVector():
            cs = self.cpu_states[cpu.id()]
            cs.agent = self.enclave().GetAgent(cpu)
            print("Agent is ready")
            if cs.agent != None:
                check = True
        if check == False:
            print("No GlobalAgent is ready")

    def Empty(self):
        return self.num_tasks_ == 0

    def Available(self, cpu):
        cs = self.cpu_states[cpu.id()]
        return cs.agent.cpu_avail()

    def GetDefaultChannel(self):
        return self.default_channel_

    def GetGlobalCPUId(self):
        return self.global_cpu

    def SetGlobalCPU(self, cpu):
        self.global_cpu = cpu

    def Enqueue(self, task):
        task.pydata.run_state = kQueued
        if not task in self.run_queue:
            if task.pydata.prio_boost or task.pydata.preempted:
                self.run_queue.appendleft(task)
            else:
                self.run_queue.append(task)

    def Dequeue(self):
        if len(self.run_queue) == 0: return None
        task = self.run_queue.popleft()
        if task != None and task.pydata.run_state == kQueued:
            task.pydata.run_state = kRunnable
            return task
        return None

    def RunqueueSize(self):
        return len(self.run_queue)

    def TaskNew(self, task, msg):
        attach_data(task)
        print("Task New")
        print(task.gtid.tid())
        payload = ghost.cast_payload_new(msg.payload())
        runtime = payload.runtime
        task.seqnum = msg.seqnum()
        task.pydata.run_state = kBlocked

        if payload.runnable:
            task.pydata.prio_boost = True
            self.Enqueue(task)

        self.num_tasks_ = self.num_tasks_ + 1


    def TaskRunnable(self, task, msg):
        print("Task Runnable")
        payload = ghost.cast_payload_wakeup(msg.payload())
        if task.pydata.run_state == kBlocked and task in self.blocked_queue:
            task.pydata.prio_boost = not payload.deferrable
            self.Enqueue(task)
            self.blocked_queue.remove(task)

    def TaskDeparted(self, task, msg):
        print("Task Departed")
        if task.pydata.run_state == kOnCpu:
            cs = self.cpu_state_of(task)
            if cs.current == task:
                cs.current = None
        elif task.pydata.run_state == kQueued:
            self.run_queue.remove(task)

        self.allocator().FreeTask(task)
        self.num_tasks_ = self.num_tasks_ - 1

    def TaskDead(self, task, msg):
        print("Task Dead")
        if task.pydata.run_state == kBlocked:
            self.allocator().FreeTask(task)
            if task in self.blocked_queue: self.blocked_queue.remove(task)
            if task in self.yielding_tasks_: self.yielding_tasks_.remove(task)
            if task in self.run_queue: self.run_queue.remove(task)
            self.num_tasks_ = self.num_tasks_ - 1

    def TaskYield(self, task, msg):
        print("Task Yield")
        if task.pydata.run_state == kOnCpu:
            cs = self.cpu_state_of(task)
            if cs.current == task:
                cs.current = None
                self.Yield(task)

    def TaskBlocked(self, task, msg):
        print("Task Blocked")
        if task.pydata.run_state == kOnCpu:
            cs = self.cpu_state_of(task)
            if cs.current == task:
                cs.current = None
        task.pydata.run_state = kBlocked
        task.pydata.prio_boost = False
        self.blocked_queue.appendleft(task)


    def TaskPreempted(self, task, msg):
        print("Task Preempt")
        print(task.gtid.tid())
        task.pydata.preempted = True
        task.pydata.prio_boost = True
        if task.pydata.run_state == kOnCpu:
            cs = self.cpu_state_of(task)
            if cs.current == task:
                cs.current = None
                self.Enqueue(task)
        elif task.pydata.run_state == kQueued:
            self.Enqueue(task)

    def Yield(self, task):
        if (task.pydata.run_state == kOnCpu) or (task.pydata.run_state == kRunnable):
            task.pydata.run_state = kYielding
            self.yielding_tasks_.appendleft(task)


    def Unyield(self, task):
        if task.pydata.run_state == kYielding:
            tmp_yielding_task = queue.deque()
            for i in range(len(self.yielding_tasks_)):
                tmp_task = self.yielding_tasks_.popleft()
                if tmp_task == task:
                    task.pydata.prio_boost = True
                    self.Enqueue(task)
                else:
                    tmp_yielding_task.appendleft(tmp_task)

            for i in range(len(tmp_yielding_task)):
                self.yielding_tasks_.appendleft(tmp_yielding_task.popleft())


    def TaskOnCpu(self, task, cpu):
        cs = self.cpu_states[cpu.id()]
        if task == cs.current:
            task.pydata.run_state = kOnCpu
            task.pydata.cpu = cpu.id()
            task.pydata.preempted = False
            task.pydata.prio_boost = False

    def UnscheduleTask(self, task):
           if task != None:
                if task.pydata.run_state == kOnCpu:
                    cpu = ghost.GetCpu(task.pydata.cpu)
                    req = self.enclave().GetRunRequest(cpu)
                    opts = ghost.RunRequestOptions()
                    opts.target = ghost.Gtid(0)
                    opts.commit_flags = ghost.COMMIT_AT_TXN_COMMIT
                    req.Open(opts)
                    if req.Commit():
                        cs = self.cpu_state_of(task)
                        cs.current = None
                        task.pydata.run_state = kOnCpu

    def SkipForSchedule(self, cpu):
           cs = self.cpu_states[cpu.id()]
           if (not self.Available(cpu)) or (cpu.id() == self.GetGlobalCPUId()): return True

           if cs.current: return True

           return False

    def PickNextGlobalCPU(self, agent_barrier):
           for cpu in self.cpus().ToVector():
                if self.Available(cpu) and cpu.id() != self.GetGlobalCPUId():
                    cs = self.cpu_states[cpu.id()]
                    prev_task = cs.current
                    if prev_task != None:
                        if prev_task.pydata.run_state == kOnCpu:
                            self.UnscheduleTask(prev_task)
                            self.Yield(prev_task)
                            return
                    self.SetGlobalCPU(cpu.id())
                    self.enclave().GetAgent(cpu).Ping()
                    break

    def GlobalSchedule(self, agent_sw, agent_barrier):
           open_cpus = ghost.GetEmptyCpuList()
           to_run = None

           updated_cpus = ghost.GetEmptyCpuList()
           for cpu in self.cpus().ToVector():
                   cs = self.cpu_states[cpu.id()]
                   if not self.SkipForSchedule(cpu):
                       to_run = self.Dequeue()
                       if to_run:
                           cs.next = to_run
                           updated_cpus.Set(cpu.id())

           for cpu in updated_cpus.ToVector():
               cs = self.cpu_states[cpu.id()]
               next = cs.next
               if next != None:
                   if cs.current == next: continue
                   req = self.enclave().GetRunRequest(cpu)
                   opts = ghost.RunRequestOptions()
                   opts.target = next.gtid
                   opts.target_barrier = next.seqnum
                   opts.commit_flags = ghost.COMMIT_AT_TXN_COMMIT
                   req.Open(opts)
                   open_cpus.Set(cpu.id())

           if not open_cpus.Empty(): self.enclave().CommitRunRequests(open_cpus)
           for cpu in open_cpus.ToVector():
                cs = self.cpu_states[cpu.id()]
                next = cs.next
                cs.next = None
                req = self.enclave().GetRunRequest(cpu)
                if req.committed():
                    if ghost.txnReqEqualToGHOST_TXN_COMPLETE(req):
                        if cs.current:
                            prev = cs.current
                            if prev.pydata.run_state == kOnCpu:
                                prev.pydata.prio_boost = True
                                self.Enqueue(prev)

                        cs.current = next
                        next.pydata.run_state = kOnCpu
                        next.pydata.cpu = cpu.id()
                        next.pydata.preempted = False
                        next.pydata.prio_boost = False
                    else:
                        next.pydata.prio_boost = True
                        self.Enqueue(next)

           if len(self.yielding_tasks_) != 0:

               for i in range(len(self.yielding_tasks_)):
                        tmp_task = self.yielding_tasks_.popleft()
                        tmp_task.pydata.prio_boost = True
                        if tmp_task.pydata.run_state == kYielding: self.Enqueue(tmp_task)


class PyAgent(ghost.LocalAgent):
    def __init__(self, enclave, cpu, scheduler):
        ghost.LocalAgent.__init__(self, enclave, cpu)
        self.enclave_ = enclave
        self.scheduler_ = scheduler

    def AgentThread(self):
        channel = self.scheduler_.GetDefaultChannel()
        self.gtid().assign_name("Agent:"+str(self.cpu().id()))
        self.SignalReady()
        self.WaitForEnclaveReady()

        while not self.Finished():
            agent_barrier = self.status_word().barrier()
            if self.cpu().id() != self.scheduler_.GetGlobalCPUId():
                req = self.enclave_.GetRunRequest(self.cpu())
                req.LocalYield(agent_barrier, 0)
            else:
                if self.boosted_priority():
                    self.scheduler_.PickNextGlobalCPU(agent_barrier)
                    continue

                msg = self.scheduler_.default_channel_.Peek()
                while not msg.empty():
                    print(msg.type())
                    self.scheduler_.DispatchMessage(msg)
                    self.scheduler_.default_channel_.Consume(msg)
                    msg = self.scheduler_.default_channel_.Peek()

                self.scheduler_.GlobalSchedule(self.status_word(), agent_barrier)

    def AgentScheduler(self):
        return self.scheduler_

class FullPyAgent(ghost.FullAgent_LocalEnclave_PyAgentConfig_):
    def __init__(self, config):
        ghost.FullAgent_LocalEnclave_PyAgentConfig_.__init__(self, config)
        global_cpu = ghost.GetCpu(1).id()
        self.scheduler_ = PyScheduler(self.enclave_, self.enclave_.cpus(), global_cpu)
        self.StartAgentTasks()
        self.enclave_.Ready()

    def __del__(self):
        self.scheduler_.ValidatePreExitState()
        self.TerminateAgentTasks()

    def MakeAgent(self, cpu):
        global nurse
        ret = PyAgent(self.enclave_, cpu, self.scheduler_)
        nurse.append(ret)
        return ret

    def RpcHandler(self, req, args, response):
        response.response_code = -1


config = ghost.getConfig();

wrapconfig = ghost.PyWrapAgentConfig(config, (lambda conf: FullPyAgent(conf)))

uap = ghost.AgentProcess_WrapFullAgent_PyWrapAgentConfig_(wrapconfig);

ghost.Ghost.InitCore()

print("Running")

input()

print("Shutting down")

del uap

print("Done!")
