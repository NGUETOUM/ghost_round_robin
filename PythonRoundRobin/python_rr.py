print("Python is starting.")

from libpython_module import *
print("Importing bindings: OK")

import queue
import time
from queue import PriorityQueue
from goto import with_goto

print("Topology:", ", ".join((str([s.id() for s in c.siblings().ToVector()]) for c in ghost.GetTopoCpuList().ToVector())))

kBlocked = 0
kQueued = 1
kOnCpu = 2
kYielding = 3
kPaused = 4

def prefix(x):
    return x*" " + str(x) + (8-x)*" "

class TaskData:
    def __init__(self):
        self.run_state = kBlocked
        self.cpu = -1
        self.preempted = False
        self.prio_boost = False
        self.qos = 0
        self.has_work = False
        self.runtime = 0
        self.elapsed_runtime = 0
        self.last_ran = time.time()

    def attach_data(task):
        if type(task.pydata) is not TaskData: task.pydata = TaskData()

    def SetRuntime(task, newRuntime, update_elapsed_runtime):
        if newRuntime > self.runtime:
            if update_elapsed_runtime:
                task.pydata.elapsed_runtime += new_runtime - task.pydata.runtime
            task.pydata.runtime = new_runtime
    pass

    def UpdateRuntime(task):
        runtime = task.status_word.runtime
        SetRuntime(self, runtime, True)
        pass

class CpuState:
    def __init__(self):
        self.current = None
        self.next = None
        self.agent = None

class PyScheduler(ghost.BasicDispatchScheduler_PyTask_):
    def __init__(self, enclave, cpulist, global_cpu, preemption_time_slice):
        print("Hello word")
        self.allocator_ = ghost.SingleThreadMallocTaskAllocator_PyTask_()
        ghost.BasicDispatchScheduler_PyTask_.__init__(self, enclave, self.allocator_, cpulist, self.allocator_)
        node = 0
        self.default_channel_ = ghost.LocalChannel(ghost.GHOST_MAX_QUEUE_ELEMS, node)
        self.preemption_time_slice = preemption_time_slice
        self.run_queue = queue.PriorityQueue()
        self.num_tasks_ = 0
        self.in_discovery_ = False
        self.yielding_tasks_ = queue.deque()
        if not self.cpus().IsSet():
            if self.cpus().Front().isValid():
                self.global_cpu = self.cpus().Front().id()

        self.cpu_states = [None for _ in range(len(self.cpus().ToVector())+1)]


    def cpu_state_of(self, task):
        return self.cpu_states[task.pydata.cpu]

    def Schedule(self):
        pass

    def EnclaveReady(self):
        check = False
        for cpu in self.cpus().ToVector():
            cs = self.cpu_states[cpu.id()]
            cs.agent = self.enclave().GetAgent(cpu)
            if cs.agent != None:
                check = True

        if check == False:
            print("No GlobalAgent is ready")
            exit()

    def Empty(self):
        return self.num_tasks_ == 0

    def Available(self, cpu):
        cs = self.cpu_states[cpu]
        return cs.agent.cpu_avail()

    def GetDefaultChannel(self):
        return self.default_channel_

    def enqueue(self, task):
        if not task.has_work:
            task.pydata.run_state = kPaused
            return

        task.pydata.run_state = kQueued
        if not task.pydata.prio_boost:
            self.run_queue.put(task.pydata.qos, task)
        else:
            task.pydata.qos.qos = 0
            self.run_queue.put(task.pydata.qos, task)
        self.run_queue.queue.sort()

    def dequeue(self):
        if self.run_queue.queue.empty(): return None
        tuple = self.run_queue.queue[0]
        task = tuple[1]
        if task != None and task.pydata.has_work != False:
            return task
        return None

    def RunqueueSize(self):
        return len(self.run_queue.queue)

    def UpdateTaskRuntime(task, new_runtime, update_elapsed_runtime):
        task.SetRuntime(new_runtime, update_elapsed_runtime)

    def DiscoveryStart(self):
        self.in_discovery_ = True

    def DiscoveryComplete(self):
        self.in_discovery_ = False

    def GetGlobalCPUId(self):
        return self.global_cpu

    def SetGlobalCPU(self, cpu):
        self.global_cpu = cpu

    def Peek(self):
        if self.run_queue.queue.empty(): return None
        tuple = self.run_queue.queue[0]
        task = tuple[1]
        if task.pydata.has_work == True:
            return task
        return None

    def RemoveFromRunqueue(self, task):
        if task.pydata.run_state == kQueued:
            queue = queue.deque()
            for i in range(len(self.run_queue.queue)):
                queue.appendleft(self.run_queue.get())
            tuple = (task.pydata.qos, task)
            queue.remove(tuple)
            for i in range(len(queue)):
                self.run_queue.put(queue.popleft())
            self.run_queue.queue.sort()
        return

    def TaskNew(self, task, msg):
        attach_data(task)
        payload = ghost.cast_payload_new(msg.payload())
        UpdateTaskRuntime(task, payload.runtime, False)
        task.seqnum = msg.seqnum()
        task.pydata.run_state = kBlocked
        task.pydata.has_work = True
        task.pydata.qos = self.num_tasks_
        if payload.runnable:
            Enqueue(self, task)

        self.num_tasks_ += self.num_tasks_

    def TaskRunnable(self, task, msg):
        payload = ghost.cast_payload_wakeup(msg.payload())
        if task.pydata.run_state == kBlocked:
            task.pydata.prio_boost = not payload.deferrable
            Enqueue(self, task)

    def TaskDeparted(self, task, msg):
        if task.pydata.run_state == kOnCpu:
            cs = cpu_state_of(self, task)
            if cs.current == task:
                cs.current = None
        elif task.pydata.run_state == kQueued:
            RemoveFromRunqueue(self, task)

        self.allocator().FreeTask(task)
        self.num_tasks_ -= self.num_tasks_

    def TaskDead(self, task, msg):
        payload = ghost.cast_payload_dead(msg.payload())
        if task.pydata.run_state == kBlocked:
            self.allocator().FreeTask(task)
            self.num_tasks_ -= self.num_tasks_

    def TaskYield(self, task, msg):
        payload = ghost.cast_payload_preempt(msg.payload())
        if payload.runtime == task.status_word.runtime():
            if task.pydata.run_state == kOnCpu:
                UpdateTaskRuntime(task, payload.runtime, True)
                cs = cpu_state_of(self, task)
                if cs.current == task:
                    cs.current = None
                    Yield(self, task)

    def TaskBlocked(self, task, msg):
        payload = ghost.cast_payload_blocked(msg.payload())
        if payload.runtime == task.status_word.runtime():
            if task.pydata.run_state == kOnCpu:
                UpdateTaskRuntime(task, payload.runtime, True)
                cs = cpu_state_of(self, task)
                if cs.current == task:
                    cs.current = None
            elif task.pydata.run_state == kQueued:
                RemoveFromRunqueue(self, task)
        task.pydata.run_state = kBlocked


    def TaskPreempted(self, task, msg):
        payload = ghost.cast_payload_preempt(msg.payload())
        if payload.runtime == task.status_word.runtime():
            task.pydata.preempted = True
            task.pydata.prio_boost = True
            if task.pydata.run_state == kOnCpu:
                UpdateTaskRuntime(task, payload.runtime, True)
                cs = cpu_state_of(self, task)
                if cs.current == task:
                    cs.current = None
                    Enqueue(self, task)
            elif task.pydata.run_state == kQueued:
                RemoveFromRunqueue(self, task)
                Enqueue(self, task)


    def Yield(self, task):
          if (task.pydata.run_state == kOnCpu) or (task.pydata.run_state == kQueued):
            task.pydata.run_state = kYielding
            self.yielding_tasks_.appendleft(task)

    def Unyield(self, task):
        if task.pydata.run_state == kYielding:
            for i in range(len(self.yielding_tasks_)):
                tmp_task = self.yielding_tasks_.popleft()
                if tmp_task == task:
                    break
                else:
                    self.yielding_tasks_.append(tmp_task)

        Enqueue(self, task)

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
                        cs = cpu_state_of(self, task)
                        cs.current = None
                        task.pydata.run_state = kPaused

    def SkipForSchedule(self, iteration, cpu):
           cs = cpu_state_of(self, task)
           if (not Available(cpu)) or (cpu.id() == GetGlobalCPUId()): return True

           if (iteration == 0) and cs.current: return True

           if (iteration == 1) and cs.next: return True

           return False

    def PickNextGlobalCPU(self, agent_barrier):
           for cpu in self.cpus().ToVector():
                if Available(cpu) and (cpu.id() != GetGlobalCPUId()):
                    cs = self.cpu_states[cpu]
                    prev_task = cs.current
                    if prev_task != None:
                        if prev_task.pydata.run_state == kOnCpu:
                            return
                    SetGlobalCPU(cpu)
                    self.enclave().GetAgent(cpu).Ping()
                    break

    def GlobalSchedule(agent_sw, agent_barrier):
           print("Nothing to schedule")
           open_cpus = ghost.GetEmptyCpuList()
           now = time.time()
           for x in range(2):
                updated_cpus = ghost.GetEmptyCpuList()
                for cpu in self.cpus().ToVector():
                    cs = self.cpu_states[cpu]
                    if SkipForSchedule(i, cpu):
                        continue
                    label .again
                    if cs.current:
                        elapsed_runtime = now - cs.current.pydata.last_ran
                        peek = Peek(self)
                        should_preempt = False
                        if peek:
                            current_qos = cs.current.pydata.qos
                            peek_qos = peek.pydata.qos
                            if current_qos < peek_qos:
                                should_preempt = True
                            elif current_qos == peek_qos:
                                if elapsed_runtime >= self.preemption_time_slice_: should_preempt = True

                        if not should_preempt: continue

                    to_run = Dequeue()
                    if not to_run: break

                    if to_run.status_word.on_cpu():
                        Yied(self, to_run)
                        goto .again

                    cs.next = to_run
                    updated_cpus.Set(cpu.id())

                for cpu in updated_cpus:
                    cs = self.cpu_states[cpu]
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

           for cpu in open_cpus:
                cs = self.cpu_states[cpu]
                next = cs.next
                cs.next = None
                req = self.enclave().GetRunRequest(cpu)
                if req.committed():
                    if req.state() == ghost.GHOST_TXN_COMPLETE:
                        if cs.current:
                            prev = cs.current
                            if prev.pydata.run_state == kOnCpu:
                                UpdateRuntime(prev)
                                Enqueue(self, prev)

                        cs.current = next
                        next.pydata.run_state = kOnCpu
                        next.pydata.cpu = cpu.id()
                        next.pydata.preempted = false
                        next.prio_boost = false

                    else: Enqueue(self, prev)

           if not self.yielding_tasks_.empty():
                 for i in range(len(self.yielding_tasks_)):
                        tmp_task = self.yielding_tasks_.pop()
                        if tmp_task.pydata.run_state == kYielding: Enqueue(self, tmp_task)


class PyAgent(ghost.LocalAgent):
    def __init__(self, enclave, cpu, scheduler):
        ghost.LocalAgent.__init__(self, enclave, cpu)
        self.scheduler_ = scheduler

    def AgentThread(self):
        self.gtid().assign_name("Agent:"+str(self.cpu().id()))
        self.SignalReady()
        self.WaitForEnclaveReady()
        while not self.Finished():
            agent_barrier = self.status_word().barrier()
            if self.cpu().id() != self.scheduler_.GetGlobalCPUId():
                req = self.enclave().GetRunRequest(self.cpu())
                req.LocalYield(agent_barrier, 0)
            else:
                if self.boosted_priority():
                    self.scheduler_.PickNextGlobalCPU(agent_barrier)
                    continue

            msg = ghost.Peek(self.scheduler_.default_channel_)
            while not msg.empty():
                self.DispatchMessage(msg)
                ghost.Consume(self.scheduler_.default_channel_, msg)
                msg = ghost.Peek(self.scheduler_.default_channel_)
                self.scheduler_.GlobalSchedule(self.status_word(), agent_barrier)


    def AgentScheduler(self):
        return self.scheduler_

class FullPyAgent(ghost.FullAgent_LocalEnclave_PyAgentConfig_):
    def __init__(self, config):
        ghost.FullAgent_LocalEnclave_PyAgentConfig_.__init__(self, config)
        global_cpu = ghost.GetCpu(1).id()
        preemption_time_slice = 5000000000
        scheduler_ = PyScheduler(self.enclave_, self.enclave_.cpus(), global_cpu, preemption_time_slice)


        self.StartAgentTasks()
        self.enclave_.Ready()

    def __del__(self):
        #self.scheduler_.ValidatePreExitState()
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
