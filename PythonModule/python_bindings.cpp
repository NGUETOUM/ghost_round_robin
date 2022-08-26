#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
namespace py = pybind11;

#include "lib/topology.h"
#include "lib/agent.h"
#include "lib/base.h"
#include "lib/channel.h"
#include "lib/enclave.h"
#include "lib/ghost.h"
#include "lib/logging.h"
#include "lib/scheduler.h"
#include "python_interface.hpp"

namespace ghost {
  struct TrPB__Agent : public Agent {
    using Agent::Agent;

    void StartBegin() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Agent
      , /* function name: */ StartBegin
        ,
      );
    }

    void StartComplete() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Agent
      , /* function name: */ StartComplete
        ,
      );
    }

    void TerminateBegin() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Agent
      , /* function name: */ TerminateBegin
        ,
      );
    }

    void TerminateComplete() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Agent
      , /* function name: */ TerminateComplete
        ,
      );
    }

    ghost::StatusWord const& status_word() const override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::StatusWord const&
      , /* parent class:  */ Agent
      , /* function name: */ status_word
        ,
      );
    }

    Scheduler* AgentScheduler() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ Scheduler*
      , /* parent class:  */ Agent
      , /* function name: */ AgentScheduler
        ,
      );
    }
  };

  struct PuPB__Agent : public Agent {
    using Agent::AgentScheduler;
  };

  struct TrPB__Channel : public Channel {
    using Channel::Channel;

    ghost::Message Peek() const override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::Message
      , /* parent class:  */ Channel
      , /* function name: */ Peek
        ,
      );
    }

    void Consume(ghost::Message const& arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ Channel
      , /* function name: */ Consume
        /* arguments: */
        , arg_0
      );
    }

    size_t max_elements() const override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ size_t
      , /* parent class:  */ Channel
      , /* function name: */ max_elements
        ,
      );
    }

    //bool AssociateTask(ghost::Gtid arg_0, int arg_1, int* arg_2) const override {
    //  PYBIND11_OVERRIDE_PURE(
    //    /* return type:   */ bool
    //  , /* parent class:  */ Channel
    //  , /* function name: */ AssociateTask
    //    /* arguments: */
    //    , arg_0
    //    , arg_1
    //    , arg_2
    //  );
    //}

    bool SetEnclaveDefault() const override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Channel
      , /* function name: */ SetEnclaveDefault
        ,
      );
    }

    int GetFd() const override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ int
      , /* parent class:  */ Channel
      , /* function name: */ GetFd
        ,
      );
    }

  };

  struct TrPB__Enclave : public Enclave {
    using Enclave::Enclave;

    ghost::RunRequest* GetRunRequest(ghost::Cpu const& arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::RunRequest*
      , /* parent class:  */ Enclave
      , /* function name: */ GetRunRequest
        /* arguments: */
        , arg_0
      );
    }

    bool CommitRunRequest(ghost::RunRequest* arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ CommitRunRequest
        /* arguments: */
        , arg_0
      );
    }

    void SubmitRunRequest(ghost::RunRequest* arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ SubmitRunRequest
        /* arguments: */
        , arg_0
      );
    }

    bool CompleteRunRequest(ghost::RunRequest* arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ CompleteRunRequest
        /* arguments: */
        , arg_0
      );
    }

    void LocalYieldRunRequest(ghost::RunRequest const* arg_0, StatusWord::BarrierToken arg_1, int arg_2) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ LocalYieldRunRequest
        /* arguments: */
        , arg_0
        , arg_1
        , arg_2
      );
    }

    bool PingRunRequest(ghost::RunRequest const* arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ PingRunRequest
        /* arguments: */
        , arg_0
      );
    }

    bool CommitRunRequests(ghost::CpuList const& arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ CommitRunRequests
        /* arguments: */
        , arg_0
      );
    }

    void SubmitRunRequests(ghost::CpuList const& arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ SubmitRunRequests
        /* arguments: */
        , arg_0
      );
    }

    bool CommitSyncRequests(ghost::CpuList const& arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ CommitSyncRequests
        /* arguments: */
        , arg_0
      );
    }

    bool SubmitSyncRequests(ghost::CpuList const& arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ bool
      , /* parent class:  */ Enclave
      , /* function name: */ SubmitSyncRequests
        /* arguments: */
        , arg_0
      );
    }

    ghost::Agent* GetAgent(ghost::Cpu const& arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::Agent*
      , /* parent class:  */ Enclave
      , /* function name: */ GetAgent
        /* arguments: */
        , arg_0
      );
    }

    void ForEachTaskStatusWord(std::function<void (ghost_status_word *, uint32_t, uint32_t)> arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ ForEachTaskStatusWord
        /* arguments: */
        , arg_0
      );
    }

    void AdvertiseOnline() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ AdvertiseOnline
        ,
      );
    }

    void PrepareToExit() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ PrepareToExit
        ,
      );
    }

    void WaitForOldAgent() override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ WaitForOldAgent
        ,
      );
    }

    void InsertBpfPrograms() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ InsertBpfPrograms
        ,
      );
    }

    void DisableMyBpfProgLoad() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ DisableMyBpfProgLoad
        ,
      );
    }

    int GetCtlFd() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ int
      , /* parent class:  */ Enclave
      , /* function name: */ GetCtlFd
        ,
      );
    }

    void SetRunnableTimeout(absl::Duration arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ SetRunnableTimeout
        /* arguments: */
        , arg_0
      );
    }

    void SetCommitAtTick(bool arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ SetCommitAtTick
        /* arguments: */
        , arg_0
      );
    }

    void SetDeliverTicks(bool arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ SetDeliverTicks
        /* arguments: */
        , arg_0
      );
    }

    void AttachAgent(ghost::Cpu const& arg_0, ghost::Agent* arg_1) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ AttachAgent
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

    void DetachAgent(ghost::Agent* arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Enclave
      , /* function name: */ DetachAgent
        /* arguments: */
        , arg_0
      );
    }

  };

  struct TrPB__FullAgent_LocalEnclave_PyAgentConfig_ : public FullAgent<LocalEnclave,PyAgentConfig> {
    using FullAgent<LocalEnclave,PyAgentConfig>::FullAgent;

    void RpcHandler(int64_t arg_0, ghost::AgentRpcArgs const& arg_1, ghost::AgentRpcResponse& arg_2) override {
      using parent = FullAgent<LocalEnclave,PyAgentConfig>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ RpcHandler
        /* arguments: */
        , arg_0
        , arg_1
        , arg_2
      );
    }

    std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) override {
      return std::unique_ptr<Agent>(MakeAgentHelper(cpu));
    }

    Agent* MakeAgentHelper(const Cpu& cpu) {
      using parent = FullAgent<LocalEnclave,PyAgentConfig>;
      py::gil_scoped_acquire acquire;
      PYBIND11_OVERLOAD_INT(
        /* return type:   */ Agent*
      , /* parent class:  */ parent
      , /* function name: */ "MakeAgent"
        /* arguments: */
        , cpu
      );
      return nullptr;
    }
  };

  class PuPB__FullAgent_LocalEnclave_PyAgentConfig_ : public FullAgent<LocalEnclave,PyAgentConfig> {
   public:
    using FullAgent<LocalEnclave,PyAgentConfig>::MakeAgent;
    using FullAgent<LocalEnclave,PyAgentConfig>::StartAgentTasks;
    using FullAgent<LocalEnclave,PyAgentConfig>::TerminateAgentTasks;
    using FullAgent<LocalEnclave,PyAgentConfig>::enclave_;
  };

  struct TrPB__LocalAgent : public LocalAgent {
    using LocalAgent::LocalAgent;

    void StartBegin() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalAgent
      , /* function name: */ StartBegin
        ,
      );
    }

    void StartComplete() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalAgent
      , /* function name: */ StartComplete
        ,
      );
    }

    void TerminateBegin() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalAgent
      , /* function name: */ TerminateBegin
        ,
      );
    }

    void TerminateComplete() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalAgent
      , /* function name: */ TerminateComplete
        ,
      );
    }

    ghost::StatusWord const& status_word() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ ghost::StatusWord const&
      , /* parent class:  */ LocalAgent
      , /* function name: */ status_word
        ,
      );
    }

   protected:
    void AgentThread() override {
      py::gil_scoped_acquire acquire;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ LocalAgent
      , /* function name: */ AgentThread
        ,
      );
    }

    Scheduler* AgentScheduler() const override {
      py::gil_scoped_acquire acquire;
      PYBIND11_OVERRIDE(
        /* return type:   */ Scheduler*
      , /* parent class:  */ LocalAgent
      , /* function name: */ AgentScheduler
        ,
      );
    }
  };

  struct PuPB__LocalAgent : public LocalAgent {
    using LocalAgent::AgentThread;
    using LocalAgent::AgentScheduler;
    using LocalAgent::SignalReady;
    using LocalAgent::WaitForEnclaveReady;
  };

  struct TrPB__LocalChannel : public LocalChannel {
    using LocalChannel::LocalChannel;

    ghost::Message Peek() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ ghost::Message
      , /* parent class:  */ LocalChannel
      , /* function name: */ Peek
        ,
      );
    }

    void Consume(ghost::Message const& arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalChannel
      , /* function name: */ Consume
        /* arguments: */
        , arg_0
      );
    }

    size_t max_elements() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ size_t
      , /* parent class:  */ LocalChannel
      , /* function name: */ max_elements
        ,
      );
    }

    //bool AssociateTask(ghost::Gtid arg_0, int arg_1, int* arg_2) const override {
    //  PYBIND11_OVERRIDE(
    //    /* return type:   */ bool
    //  , /* parent class:  */ LocalChannel
    //  , /* function name: */ AssociateTask
    //    /* arguments: */
    //    , arg_0
    //    , arg_1
    //    , arg_2
    //  );
    //}

    bool SetEnclaveDefault() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ bool
      , /* parent class:  */ LocalChannel
      , /* function name: */ SetEnclaveDefault
        ,
      );
    }

    int GetFd() const override {
      PYBIND11_OVERRIDE(
        /* return type:   */ int
      , /* parent class:  */ LocalChannel
      , /* function name: */ GetFd
        ,
      );
    }

  };

  struct TrPB__Scheduler : public Scheduler {
    using Scheduler::Scheduler;

    void EnclaveReady() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Scheduler
      , /* function name: */ EnclaveReady
        ,
      );
    }

    void DiscoverTasks() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Scheduler
      , /* function name: */ DiscoverTasks
        ,
      );
    }

    ghost::Channel& GetDefaultChannel() override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::Channel&
      , /* parent class:  */ Scheduler
      , /* function name: */ GetDefaultChannel
        ,
      );
    }

    void DumpState(ghost::Cpu const& arg_0, int arg_1) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ Scheduler
      , /* function name: */ DumpState
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

  };

  struct PuPB__Scheduler : public Scheduler {
    using Scheduler::cpus;
  };

  struct TrPB__SingleThreadMallocTaskAllocator_PyTask_ : public SingleThreadMallocTaskAllocator<PyTask> {
    using SingleThreadMallocTaskAllocator<PyTask>::SingleThreadMallocTaskAllocator;

    PyTask* GetTask(ghost::Gtid arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ PyTask*
      , /* parent class:  */ SingleThreadMallocTaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
      );
    }

    std::tuple<PyTask *, bool> GetTask(ghost::Gtid arg_0, ghost_sw_info arg_1) override {
      using ret = std::tuple<PyTask *, bool>;
      PYBIND11_OVERRIDE(
        /* return type:   */ ret
      , /* parent class:  */ SingleThreadMallocTaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

    void FreeTask(PyTask* arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ SingleThreadMallocTaskAllocator<PyTask>
      , /* function name: */ FreeTask
        /* arguments: */
        , arg_0
      );
    }

    void ForEachTask(typename TaskAllocator<PyTask>::TaskCallbackFunc arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ SingleThreadMallocTaskAllocator<PyTask>
      , /* function name: */ ForEachTask
        /* arguments: */
        , arg_0
      );
    }

  };

  struct TrPB__StatusWord : public StatusWord {
    using StatusWord::StatusWord;

    void Free() override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ StatusWord
      , /* function name: */ Free
        ,
      );
    }

  };

  struct TrPB__TaskAllocator_PyTask_ : public TaskAllocator<PyTask> {
    using TaskAllocator<PyTask>::TaskAllocator;

    PyTask* GetTask(ghost::Gtid arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ PyTask*
      , /* parent class:  */ TaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
      );
    }

    std::tuple<PyTask *, bool> GetTask(ghost::Gtid arg_0, ghost_sw_info arg_1) override {
      using ret = std::tuple<PyTask *, bool>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ret
      , /* parent class:  */ TaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

    void FreeTask(PyTask* arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ TaskAllocator<PyTask>
      , /* function name: */ FreeTask
        /* arguments: */
        , arg_0
      );
    }

    void ForEachTask(ghost::TaskAllocator<PyTask>::TaskCallbackFunc arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ TaskAllocator<PyTask>
      , /* function name: */ ForEachTask
        /* arguments: */
        , arg_0
      );
    }

  };

  struct TrPB__ThreadSafeMallocTaskAllocator_PyTask_ : public ThreadSafeMallocTaskAllocator<PyTask> {
    using ThreadSafeMallocTaskAllocator<PyTask>::ThreadSafeMallocTaskAllocator;

    void ForEachTask(ghost::TaskAllocator<PyTask>::TaskCallbackFunc arg_0) override {
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ ThreadSafeMallocTaskAllocator<PyTask>
      , /* function name: */ ForEachTask
        /* arguments: */
        , arg_0
      );
    }

    PyTask* GetTask(ghost::Gtid arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ PyTask*
      , /* parent class:  */ ThreadSafeMallocTaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
      );
    }

    std::tuple<PyTask *, bool> GetTask(ghost::Gtid arg_0, ghost_sw_info arg_1) override {
      using ret = std::tuple<PyTask *, bool>;
      PYBIND11_OVERRIDE(
        /* return type:   */ ret
      , /* parent class:  */ ThreadSafeMallocTaskAllocator<PyTask>
      , /* function name: */ GetTask
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

    void FreeTask(PyTask* arg_0) override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ ThreadSafeMallocTaskAllocator<PyTask>
      , /* function name: */ FreeTask
        /* arguments: */
        , arg_0
      );
    }

    // Note: this duplicate should not exist
    //void ForEachTask(typename TaskAllocator<PyTask>::TaskCallbackFunc arg_0) override {
    //  PYBIND11_OVERRIDE(
    //    /* return type:   */ void
    //  , /* parent class:  */ ThreadSafeMallocTaskAllocator<PyTask>
    //  , /* function name: */ ForEachTask
    //    /* arguments: */
    //    , arg_0
    //  );
    //}

  };

  struct TrPB__BasicDispatchScheduler_PyTask_ : public BasicDispatchScheduler<PyTask> {
    using BasicDispatchScheduler<PyTask>::BasicDispatchScheduler;

    void EnclaveReady() override {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ EnclaveReady
        ,
      );
    }

    ghost::Channel& GetDefaultChannel() override {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ ghost::Channel&
      , /* parent class:  */ parent
      , /* function name: */ GetDefaultChannel
        ,
      );
    }

    void DumpState(ghost::Cpu const& arg_0, int arg_1) override {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ DumpState
        /* arguments: */
        , arg_0
        , arg_1
      );
    }

    void DispatchMessage(ghost::Message const& arg_0) override {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ DispatchMessage
        /* arguments: */
        , arg_0
      );
    }

    void DiscoverTasks() override {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ DiscoverTasks
        ,
      );
    }

    void TaskNew(PyTask* task, const Message& msg) {
      py::gil_scoped_acquire acquire;
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskNew
        /* arguments: */
        , task
        , msg
      );
      while(true);
    }

    void TaskRunnable(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskRunnable
        /* arguments: */
        , task
        , msg
      );
    }

    void TaskDead(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskDead
        /* arguments: */
        , task
        , msg
      );
    }

    void TaskDeparted(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskDeparted
        /* arguments: */
        , task
        , msg
      );
    }

    void TaskYield(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskYield
        /* arguments: */
        , task
        , msg
      );
    }

    void TaskBlocked(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskBlocked
        /* arguments: */
        , task
        , msg
      );
    }

    void TaskPreempted(PyTask* task, const Message& msg) {
      using parent = BasicDispatchScheduler<PyTask>;
      PYBIND11_OVERRIDE_PURE(
        /* return type:   */ void
      , /* parent class:  */ parent
      , /* function name: */ TaskPreempted
        /* arguments: */
        , task
        , msg
      );
    }

  };

  struct PuPB__BasicDispatchScheduler_PyTask_ : public BasicDispatchScheduler<PyTask> {
    using BasicDispatchScheduler<PyTask>::TaskNew;
    using BasicDispatchScheduler<PyTask>::TaskRunnable;
    using BasicDispatchScheduler<PyTask>::TaskDead;
    using BasicDispatchScheduler<PyTask>::TaskDeparted;
    using BasicDispatchScheduler<PyTask>::TaskYield;
    using BasicDispatchScheduler<PyTask>::TaskBlocked;
    using BasicDispatchScheduler<PyTask>::TaskPreempted;
    using BasicDispatchScheduler<PyTask>::enclave;
    using BasicDispatchScheduler<PyTask>::cpus;
    using BasicDispatchScheduler<PyTask>::allocator;
  };

  struct TrPB__LocalStatusWord : public LocalStatusWord {
    using LocalStatusWord::LocalStatusWord;

    void Free() override {
      PYBIND11_OVERRIDE(
        /* return type:   */ void
      , /* parent class:  */ LocalStatusWord
      , /* function name: */ Free
        ,
      );
    }

  };

}

PYBIND11_MODULE(libpython_module, PB__m) {
  py::module PB__ghost = PB__m.def_submodule("ghost"); {
    using namespace ghost;
    py::class_<Agent, TrPB__Agent> PB__Agent(PB__ghost, "Agent"); {
      PB__Agent.def("StartBegin", &Agent::StartBegin);
      PB__Agent.def("StartComplete", &Agent::StartComplete);
      PB__Agent.def("Start", &Agent::Start);
      PB__Agent.def("TerminateBegin", &Agent::TerminateBegin);
      PB__Agent.def("TerminateComplete", &Agent::TerminateComplete);
      PB__Agent.def("Terminate", &Agent::Terminate);
      PB__Agent.def("Finished", &Agent::Finished);
      PB__Agent.def("cpu", &Agent::cpu);
      PB__Agent.def("Ping", &Agent::Ping);
      PB__Agent.def("gtid", &Agent::gtid);
      PB__Agent.def("cpu_avail", &Agent::cpu_avail);
      PB__Agent.def("boosted_priority", &Agent::boosted_priority);
      PB__Agent.def("barrier", &Agent::barrier);
      PB__Agent.def("enclave", &Agent::enclave);
      PB__Agent.def("status_word", &Agent::status_word);
      PB__Agent.def("AgentScheduler", &PuPB__Agent::AgentScheduler);
    }

    py::class_<AgentConfig> PB__AgentConfig(PB__ghost, "AgentConfig"); {
//      PB__AgentConfig.def(py::init<ghost::Topology*, ghost::CpuList>());
//      PB__AgentConfig.def_readwrite("topology_", &AgentConfig::topology_);
      PB__AgentConfig.def_readwrite("cpus_", &AgentConfig::cpus_);
      PB__AgentConfig.def_readwrite("enclave_fd_", &AgentConfig::enclave_fd_);
      PB__AgentConfig.def_readwrite("tick_config_", &AgentConfig::tick_config_);
      PB__AgentConfig.def_readwrite("stderr_fd_", &AgentConfig::stderr_fd_);
      PB__AgentConfig.def_readwrite("mlockall_", &AgentConfig::mlockall_);
    }

    py::class_<AgentProcess<WrapFullAgent,PyWrapAgentConfig>> PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_(PB__ghost, "AgentProcess_WrapFullAgent_PyWrapAgentConfig_"); {
      PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_.def(py::init<PyWrapAgentConfig>(), py::call_guard<py::gil_scoped_release>());
      PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_.def("Rpc", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::Rpc);
      PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_.def("RpcWithResponse", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::RpcWithResponse);
      PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_.def("AddExitHandler", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::AddExitHandler);
      //PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_.def("KillChild", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::KillChild);
      py::class_<AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob> PB__SharedBlob(PB__AgentProcess_WrapFullAgent_PyWrapAgentConfig_, "SharedBlob"); {
        PB__SharedBlob.def(py::init<>());
        //PB__SharedBlob.def_readwrite("agent_ready_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::agent_ready_);
        //PB__SharedBlob.def_readwrite("kill_agent_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::kill_agent_);
        //PB__SharedBlob.def_readwrite("rpc_req_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::rpc_req_);
        //PB__SharedBlob.def_readwrite("rpc_args_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::rpc_args_);
        //PB__SharedBlob.def_readwrite("rpc_res_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::rpc_res_);
        //PB__SharedBlob.def_readwrite("rpc_pending_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::rpc_pending_);
        //PB__SharedBlob.def_readwrite("rpc_done_", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::rpc_done_);
        //PB__SharedBlob.def_static("operator_new", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::operator new);
        //PB__SharedBlob.def_static("operator_delete", &AgentProcess<WrapFullAgent,PyWrapAgentConfig>::SharedBlob::operator delete);
      }

    }

    py::class_<AgentRpcArgs> PB__AgentRpcArgs(PB__ghost, "AgentRpcArgs"); {
      PB__AgentRpcArgs.def_readwrite("arg0", &AgentRpcArgs::arg0);
      PB__AgentRpcArgs.def_readwrite("arg1", &AgentRpcArgs::arg1);
      PB__AgentRpcArgs.def_readwrite("buffer", &AgentRpcArgs::buffer);
    }

    py::class_<AgentRpcResponse> PB__AgentRpcResponse(PB__ghost, "AgentRpcResponse"); {
      PB__AgentRpcResponse.def_readwrite("response_code", &AgentRpcResponse::response_code);
      PB__AgentRpcResponse.def_readwrite("buffer", &AgentRpcResponse::buffer);
    }

    py::class_<Channel, TrPB__Channel> PB__Channel(PB__ghost, "Channel"); {
      PB__Channel.def("Peek", &Channel::Peek);
      PB__Channel.def("Consume", &Channel::Consume);
      PB__Channel.def("max_elements", &Channel::max_elements);
      PB__Channel.def("AssociateTask", &Channel::AssociateTask);
      PB__Channel.def("SetEnclaveDefault", &Channel::SetEnclaveDefault);
      PB__Channel.def("GetFd", &Channel::GetFd);
    }

    py::class_<Cpu> PB__Cpu(PB__ghost, "Cpu"); {
      PB__Cpu.def(py::init<ghost::Cpu::UninitializedType>());
      PB__Cpu.def("id", &Cpu::id);
      PB__Cpu.def("core", &Cpu::core);
      PB__Cpu.def("smt_idx", &Cpu::smt_idx);
      PB__Cpu.def("valid", &Cpu::valid);
      PB__Cpu.def("siblings", &Cpu::siblings);
      PB__Cpu.def("l3_siblings", &Cpu::l3_siblings);
      PB__Cpu.def("numa_node", &Cpu::numa_node);
      PB__Cpu.def("operator__", &Cpu::operator==);
      PB__Cpu.def("operator__", &Cpu::operator!=);
      PB__Cpu.def("operator_", &Cpu::operator<);
      py::class_<Cpu::Raw> PB__Raw(PB__Cpu, "Raw"); {
        PB__Raw.def_readwrite("cpu", &Cpu::Raw::cpu);
        PB__Raw.def_readwrite("core", &Cpu::Raw::core);
        PB__Raw.def_readwrite("smt_idx", &Cpu::Raw::smt_idx);
        PB__Raw.def_readwrite("siblings", &Cpu::Raw::siblings);
        PB__Raw.def_readwrite("l3_siblings", &Cpu::Raw::l3_siblings);
        PB__Raw.def_readwrite("numa_node", &Cpu::Raw::numa_node);
        PB__Raw.def("operator__", &Cpu::Raw::operator==);
        PB__Raw.def("operator__", &Cpu::Raw::operator!=);
        PB__Raw.def("operator_", &Cpu::Raw::operator<);
      }

    }

    py::class_<CpuList> PB__CpuList(PB__ghost, "CpuList"); {
//      PB__CpuList.def(py::init<ghost::Topology const&>());
      PB__CpuList.def("operator__", &CpuList::operator==);
      PB__CpuList.def("operator__", &CpuList::operator+=);
      PB__CpuList.def("operator__", &CpuList::operator-=);
      PB__CpuList.def("ToVector", &CpuList::ToVector);
      PB__CpuList.def("ToIntVector", &CpuList::ToIntVector);
      PB__CpuList.def("Intersection", &CpuList::Intersection);
      PB__CpuList.def("Union", &CpuList::Union);
      PB__CpuList.def("Subtract", &CpuList::Subtract);
      PB__CpuList.def("Set", py::overload_cast<uint32_t>(&CpuList::Set));
      PB__CpuList.def("Set", py::overload_cast<ghost::Cpu const&>(&CpuList::Set));
      PB__CpuList.def("Clear", py::overload_cast<uint32_t>(&CpuList::Clear));
      PB__CpuList.def("Clear", py::overload_cast<ghost::Cpu const&>(&CpuList::Clear));
      PB__CpuList.def("IsSet", py::overload_cast<uint32_t>(&CpuList::IsSet, py::const_));
      PB__CpuList.def("IsSet", py::overload_cast<ghost::Cpu const&>(&CpuList::IsSet, py::const_));
      PB__CpuList.def("GetNthCpu", &CpuList::GetNthCpu);
      PB__CpuList.def("operator__", &CpuList::operator[]);
      PB__CpuList.def("Front", &CpuList::Front);
      PB__CpuList.def("Back", &CpuList::Back);
      PB__CpuList.def("Empty", &CpuList::Empty);
      PB__CpuList.def("Size", &CpuList::Size);
//      PB__CpuList.def("topology", &CpuList::topology);
      PB__CpuList.def("begin", &CpuList::begin);
      PB__CpuList.def("end", &CpuList::end);
      PB__CpuList.def("CpuMaskStr", &CpuList::CpuMaskStr);
      py::class_<CpuList::Iter> PB__Iter(PB__CpuList, "Iter"); {
//        PB__Iter.def(py::init<ghost::Topology const&, uint64_t const*, size_t, size_t>());
        PB__Iter.def("operator__", &CpuList::Iter::operator==);
        PB__Iter.def("operator__", &CpuList::Iter::operator!=);
        PB__Iter.def("operator__", &CpuList::Iter::operator->);
        PB__Iter.def("operator_", &CpuList::Iter::operator*);
        PB__Iter.def("operator__", py::overload_cast<>(&CpuList::Iter::operator++));
        PB__Iter.def("operator__", py::overload_cast<int>(&CpuList::Iter::operator++));
      }

    }

    py::class_<Enclave, TrPB__Enclave> PB__Enclave(PB__ghost, "Enclave"); {
      PB__Enclave.def(py::init<ghost::AgentConfig const>());
      PB__Enclave.def("GetRunRequest", &Enclave::GetRunRequest, py::return_value_policy::reference);
      PB__Enclave.def("CommitRunRequest", &Enclave::CommitRunRequest);
      PB__Enclave.def("SubmitRunRequest", &Enclave::SubmitRunRequest);
      PB__Enclave.def("CompleteRunRequest", &Enclave::CompleteRunRequest);
      PB__Enclave.def("LocalYieldRunRequest", &Enclave::LocalYieldRunRequest);
      PB__Enclave.def("PingRunRequest", &Enclave::PingRunRequest);
      PB__Enclave.def("CommitRunRequests", &Enclave::CommitRunRequests);
      PB__Enclave.def("SubmitRunRequests", &Enclave::SubmitRunRequests);
      PB__Enclave.def("CommitSyncRequests", &Enclave::CommitSyncRequests);
      PB__Enclave.def("SubmitSyncRequests", &Enclave::SubmitSyncRequests);
      PB__Enclave.def("GetAgent", &Enclave::GetAgent, py::return_value_policy::reference);
      PB__Enclave.def("ForEachTaskStatusWord", &Enclave::ForEachTaskStatusWord);
      PB__Enclave.def("AdvertiseOnline", &Enclave::AdvertiseOnline);
      PB__Enclave.def("PrepareToExit", &Enclave::PrepareToExit);
      PB__Enclave.def("WaitForOldAgent", &Enclave::WaitForOldAgent);
      PB__Enclave.def("InsertBpfPrograms", &Enclave::InsertBpfPrograms);
      PB__Enclave.def("DisableMyBpfProgLoad", &Enclave::DisableMyBpfProgLoad);
      PB__Enclave.def("GetCtlFd", &Enclave::GetCtlFd);
      PB__Enclave.def("SetRunnableTimeout", &Enclave::SetRunnableTimeout);
      PB__Enclave.def("SetCommitAtTick", &Enclave::SetCommitAtTick);
      PB__Enclave.def("SetDeliverTicks", &Enclave::SetDeliverTicks);
      PB__Enclave.def("Ready", &Enclave::Ready);
//      PB__Enclave.def("topology", &Enclave::topology);
      PB__Enclave.def("cpus", &Enclave::cpus, py::return_value_policy::reference);
      PB__Enclave.def("AttachAgent", &Enclave::AttachAgent);
      PB__Enclave.def("DetachAgent", &Enclave::DetachAgent);
    }

    py::class_<ForkedProcess> PB__ForkedProcess(PB__ghost, "ForkedProcess"); {
      PB__ForkedProcess.def(py::init<int>());
      PB__ForkedProcess.def(py::init<std::function<int ()>>());
      PB__ForkedProcess.def("WaitForChildExit", &ForkedProcess::WaitForChildExit);
      PB__ForkedProcess.def("IsChild", &ForkedProcess::IsChild);
      PB__ForkedProcess.def("AddExitHandler", &ForkedProcess::AddExitHandler);
      PB__ForkedProcess.def("KillChild", &ForkedProcess::KillChild);
    }

    py::class_<FullAgent<LocalEnclave,PyAgentConfig>, TrPB__FullAgent_LocalEnclave_PyAgentConfig_> PB__FullAgent_LocalEnclave_PyAgentConfig_(PB__ghost, "FullAgent_LocalEnclave_PyAgentConfig_"); {
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def(py::init<PyAgentConfig>());
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def("RpcHandler", &FullAgent<LocalEnclave,PyAgentConfig>::RpcHandler);
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def("MakeAgent", [](FullAgent<LocalEnclave,PyAgentConfig>* obj, const Cpu& cpu)->Agent* {
        return dynamic_cast<TrPB__FullAgent_LocalEnclave_PyAgentConfig_*>(obj)->MakeAgentHelper(cpu);
            });
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def("StartAgentTasks", &PuPB__FullAgent_LocalEnclave_PyAgentConfig_::StartAgentTasks, py::call_guard<py::gil_scoped_release>());
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def("TerminateAgentTasks", &PuPB__FullAgent_LocalEnclave_PyAgentConfig_::TerminateAgentTasks);
      PB__FullAgent_LocalEnclave_PyAgentConfig_.def_readonly("enclave_", &PuPB__FullAgent_LocalEnclave_PyAgentConfig_::enclave_, py::return_value_policy::reference);
    }

    py::class_<Futex> PB__Futex(PB__ghost, "Futex"); {
    }

    py::class_<Ghost> PB__Ghost(PB__ghost, "Ghost"); {
      PB__Ghost.def_readonly_static("kGhostfsMount", &Ghost::kGhostfsMount);
      PB__Ghost.def_static("InitCore", &Ghost::InitCore);
      PB__Ghost.def_static("Run", &Ghost::Run);
      PB__Ghost.def_static("SyncCommit", &Ghost::SyncCommit);
      PB__Ghost.def_static("Commit_static", py::overload_cast<cpu_set_t* const>(&Ghost::Commit));
      PB__Ghost.def_static("Commit_static", py::overload_cast<int const>(&Ghost::Commit));
      PB__Ghost.def_static("CreateQueue", &Ghost::CreateQueue);
      PB__Ghost.def_static("ConfigQueueWakeup", &Ghost::ConfigQueueWakeup);
      PB__Ghost.def_static("AssociateQueue", &Ghost::AssociateQueue);
      PB__Ghost.def_static("SetDefaultQueue", &Ghost::SetDefaultQueue);
      PB__Ghost.def_static("GetStatusWordInfo", &Ghost::GetStatusWordInfo);
      PB__Ghost.def_static("FreeStatusWordInfo", &Ghost::FreeStatusWordInfo);
      PB__Ghost.def_static("GetTaskRuntime", &Ghost::GetTaskRuntime);
      PB__Ghost.def_static("TimerFdSettime", &Ghost::TimerFdSettime);
      PB__Ghost.def_static("GhostIsMountedAt", &Ghost::GhostIsMountedAt);
      PB__Ghost.def_static("MountGhostfs", &Ghost::MountGhostfs);
      PB__Ghost.def_static("GetSupportedVersions", &Ghost::GetSupportedVersions);
      PB__Ghost.def_static("CheckVersion", &Ghost::CheckVersion);
      PB__Ghost.def_static("SetGlobalEnclaveFds", &Ghost::SetGlobalEnclaveFds);
      PB__Ghost.def_static("GetGlobalEnclaveCtlFd", &Ghost::GetGlobalEnclaveCtlFd);
      PB__Ghost.def_static("GetGlobalEnclaveDirFd", &Ghost::GetGlobalEnclaveDirFd);
      PB__Ghost.def_static("CloseGlobalEnclaveFds", &Ghost::CloseGlobalEnclaveFds);
      PB__Ghost.def_static("SetGlobalStatusWordTable", &Ghost::SetGlobalStatusWordTable);
      PB__Ghost.def_static("GetGlobalStatusWordTable", &Ghost::GetGlobalStatusWordTable);
      PB__Ghost.def_static("SchedGetAffinity", &Ghost::SchedGetAffinity);
      PB__Ghost.def_static("SchedSetAffinity", &Ghost::SchedSetAffinity);
    }

    py::class_<GhostSignals> PB__GhostSignals(PB__ghost, "GhostSignals"); {
      PB__GhostSignals.def_static("Init", &GhostSignals::Init);
      PB__GhostSignals.def_static("IgnoreCommon", &GhostSignals::IgnoreCommon);
      PB__GhostSignals.def_static("AddHandler", &GhostSignals::AddHandler);
    }

    py::class_<GhostThread> PB__GhostThread(PB__ghost, "GhostThread"); {
      PB__GhostThread.def(py::init<ghost::GhostThread::KernelScheduler, std::function<void ()>>());
      PB__GhostThread.def("Join", &GhostThread::Join);
      PB__GhostThread.def("Joinable", &GhostThread::Joinable);
      PB__GhostThread.def("tid", &GhostThread::tid);
      PB__GhostThread.def("gtid", &GhostThread::gtid);
      PB__GhostThread.def_static("SetGlobalEnclaveFdsOnce", &GhostThread::SetGlobalEnclaveFdsOnce);
    }

    py::class_<Gtid> PB__Gtid(PB__ghost, "Gtid"); {
      PB__Gtid.def(py::init<>());
      PB__Gtid.def(py::init<int64_t>());
      PB__Gtid.def_static("Current", &Gtid::Current);
      PB__Gtid.def("id", &Gtid::id);
      PB__Gtid.def("tid", &Gtid::tid);
      PB__Gtid.def("tgid", &Gtid::tgid);
      PB__Gtid.def("operator__", &Gtid::operator==);
      PB__Gtid.def("operator__", &Gtid::operator!=);
      PB__Gtid.def("operator_", &Gtid::operator!);
      PB__Gtid.def("assign_name", &Gtid::assign_name);
      PB__Gtid.def("describe", &Gtid::describe);
    }

    py::class_<LocalAgent, Agent, TrPB__LocalAgent> PB__LocalAgent(PB__ghost, "LocalAgent"); {
      PB__LocalAgent.def(py::init<ghost::Enclave*, ghost::Cpu const&>());
      PB__LocalAgent.def("StartBegin", &LocalAgent::StartBegin);
      PB__LocalAgent.def("StartComplete", &LocalAgent::StartComplete);
      PB__LocalAgent.def("Start", &LocalAgent::Start);
      PB__LocalAgent.def("TerminateBegin", &LocalAgent::TerminateBegin);
      PB__LocalAgent.def("TerminateComplete", &LocalAgent::TerminateComplete);
      PB__LocalAgent.def("Terminate", &LocalAgent::Terminate);
      PB__LocalAgent.def("Finished", &LocalAgent::Finished);
      PB__LocalAgent.def("cpu", &LocalAgent::cpu);
      PB__LocalAgent.def("Ping", &LocalAgent::Ping);
      PB__LocalAgent.def("gtid", &LocalAgent::gtid);
      PB__LocalAgent.def("cpu_avail", &LocalAgent::cpu_avail);
      PB__LocalAgent.def("boosted_priority", &LocalAgent::boosted_priority);
      PB__LocalAgent.def("barrier", &LocalAgent::barrier);
      PB__LocalAgent.def("enclave", &LocalAgent::enclave);
      PB__LocalAgent.def("status_word", &LocalAgent::status_word, py::return_value_policy::reference);
      PB__LocalAgent.def("AgentThread", &PuPB__LocalAgent::AgentThread);
      PB__LocalAgent.def("AgentScheduler", &PuPB__LocalAgent::AgentScheduler);
      PB__LocalAgent.def("SignalReady", &PuPB__LocalAgent::SignalReady);
      PB__LocalAgent.def("WaitForEnclaveReady", &PuPB__LocalAgent::WaitForEnclaveReady, py::call_guard<py::gil_scoped_release>());
    }

    py::class_<LocalChannel, Channel, TrPB__LocalChannel> PB__LocalChannel(PB__ghost, "LocalChannel"); {
      PB__LocalChannel.def(py::init<int, int, ghost::CpuList>());
      PB__LocalChannel.def("Peek", &LocalChannel::Peek);
      PB__LocalChannel.def("Consume", &LocalChannel::Consume);
      PB__LocalChannel.def("max_elements", &LocalChannel::max_elements);
      //PB__LocalChannel.def("AssociateTask", &LocalChannel::AssociateTask);
      PB__LocalChannel.def("AssociateTask2", [](LocalChannel* obj, ghost::Gtid gtid, int barrier)->bool { return obj->AssociateTask(gtid, barrier, nullptr);} );
      PB__LocalChannel.def("SetEnclaveDefault", &LocalChannel::SetEnclaveDefault);
      PB__LocalChannel.def("GetFd", &LocalChannel::GetFd);
    }

    py::class_<LocalEnclave, Enclave> PB__LocalEnclave(PB__ghost, "LocalEnclave"); {
      PB__LocalEnclave.def(py::init<ghost::AgentConfig>());
      PB__LocalEnclave.def("ForEachTaskStatusWord", py::overload_cast<std::function<void (ghost_status_word *, uint32_t, uint32_t)>>(&LocalEnclave::ForEachTaskStatusWord));
      PB__LocalEnclave.def("Ready", &LocalEnclave::Ready);
//      PB__LocalEnclave.def("topology", &LocalEnclave::topology);
      PB__LocalEnclave.def("cpus", &LocalEnclave::cpus, py::return_value_policy::reference);
      PB__LocalEnclave.def("GetRunRequest", &LocalEnclave::GetRunRequest, py::return_value_policy::reference);
      PB__LocalEnclave.def("CommitRunRequest", &LocalEnclave::CommitRunRequest);
      PB__LocalEnclave.def("SubmitRunRequest", &LocalEnclave::SubmitRunRequest);
      PB__LocalEnclave.def("CompleteRunRequest", &LocalEnclave::CompleteRunRequest);
      PB__LocalEnclave.def("LocalYieldRunRequest", &LocalEnclave::LocalYieldRunRequest);
      PB__LocalEnclave.def("PingRunRequest", &LocalEnclave::PingRunRequest);
      PB__LocalEnclave.def("CommitRunRequests", &LocalEnclave::CommitRunRequests);
      PB__LocalEnclave.def("SubmitRunRequests", &LocalEnclave::SubmitRunRequests);
      PB__LocalEnclave.def("CommitSyncRequests", &LocalEnclave::CommitSyncRequests);
      PB__LocalEnclave.def("SubmitSyncRequests", &LocalEnclave::SubmitSyncRequests);
      PB__LocalEnclave.def("GetAgent", &LocalEnclave::GetAgent, py::return_value_policy::reference);
      PB__LocalEnclave.def("AttachAgent", &LocalEnclave::AttachAgent);
      PB__LocalEnclave.def("DetachAgent", &LocalEnclave::DetachAgent);
      PB__LocalEnclave.def("GetNrTasks", py::overload_cast<>(&LocalEnclave::GetNrTasks));
      PB__LocalEnclave.def("GetAbiVersion", py::overload_cast<>(&LocalEnclave::GetAbiVersion));
      PB__LocalEnclave.def("SetRunnableTimeout", &LocalEnclave::SetRunnableTimeout);
      PB__LocalEnclave.def("SetCommitAtTick", &LocalEnclave::SetCommitAtTick);
      PB__LocalEnclave.def("SetDeliverTicks", &LocalEnclave::SetDeliverTicks);
      PB__LocalEnclave.def("ForEachTaskStatusWord", py::overload_cast<std::function<void (ghost_status_word *, uint32_t, uint32_t)> const>(&LocalEnclave::ForEachTaskStatusWord));
      PB__LocalEnclave.def("AdvertiseOnline", &LocalEnclave::AdvertiseOnline);
      PB__LocalEnclave.def("PrepareToExit", &LocalEnclave::PrepareToExit);
      PB__LocalEnclave.def("WaitForOldAgent", &LocalEnclave::WaitForOldAgent);
      PB__LocalEnclave.def("InsertBpfPrograms", &LocalEnclave::InsertBpfPrograms);
      PB__LocalEnclave.def("DisableMyBpfProgLoad", &LocalEnclave::DisableMyBpfProgLoad);
      PB__LocalEnclave.def("GetCtlFd", &LocalEnclave::GetCtlFd);
      PB__LocalEnclave.def_static("MakeNextEnclave", &LocalEnclave::MakeNextEnclave);
      PB__LocalEnclave.def_static("GetEnclaveDirectory", &LocalEnclave::GetEnclaveDirectory);
      PB__LocalEnclave.def_static("WriteEnclaveTunable", &LocalEnclave::WriteEnclaveTunable);
      PB__LocalEnclave.def_static("GetCpuDataRegion", &LocalEnclave::GetCpuDataRegion);
      PB__LocalEnclave.def_static("WaitForAgentOnlineValue", &LocalEnclave::WaitForAgentOnlineValue);
      PB__LocalEnclave.def_static("GetNrTasks_static", py::overload_cast<int>(&LocalEnclave::GetNrTasks));
      PB__LocalEnclave.def_static("GetAbiVersion_static", py::overload_cast<int>(&LocalEnclave::GetAbiVersion));
      PB__LocalEnclave.def_static("DestroyEnclave", &LocalEnclave::DestroyEnclave);
      PB__LocalEnclave.def_static("DestroyAllEnclaves", &LocalEnclave::DestroyAllEnclaves);
    }

    py::class_<Message> PB__Message(PB__ghost, "Message"); {
      PB__Message.def(py::init<>());
      PB__Message.def(py::init<ghost_msg const*>());
      PB__Message.def("type", &Message::type);
      PB__Message.def("length", &Message::length);
      PB__Message.def("seqnum", &Message::seqnum);
      PB__Message.def("payload", &Message::payload);
      PB__Message.def("empty", &Message::empty);
      PB__Message.def("is_cpu_msg", &Message::is_cpu_msg);
      PB__Message.def("cpu", &Message::cpu);
      PB__Message.def("is_task_msg", &Message::is_task_msg);
      PB__Message.def("gtid", &Message::gtid);
      PB__Message.def("describe_type", &Message::describe_type);
      PB__Message.def("stringify", &Message::stringify);
      PB__Message.def("msg", &Message::msg);
      PB__Message.def("operator__", &Message::operator==);
    }

    py::class_<Notification> PB__Notification(PB__ghost, "Notification"); {
      PB__Notification.def(py::init<>());
      PB__Notification.def("HasBeenNotified", &Notification::HasBeenNotified);
      PB__Notification.def("Reset", &Notification::Reset);
      PB__Notification.def("Notify", &Notification::Notify);
      PB__Notification.def("WaitForNotification", &Notification::WaitForNotification);
    }

    py::class_<PeriodicEdge> PB__PeriodicEdge(PB__ghost, "PeriodicEdge"); {
      PB__PeriodicEdge.def(py::init<absl::Duration>());
      PB__PeriodicEdge.def("Edge", &PeriodicEdge::Edge);
    }

    py::class_<PyAgentConfig, AgentConfig> PB__PyAgentConfig(PB__ghost, "PyAgentConfig"); {
      PB__PyAgentConfig.def(py::init<>());
//      PB__PyAgentConfig.def_readwrite("topology_", &PyAgentConfig::topology_);
      PB__PyAgentConfig.def_readwrite("cpus_", &PyAgentConfig::cpus_);
      PB__PyAgentConfig.def_readwrite("enclave_fd_", &PyAgentConfig::enclave_fd_);
      PB__PyAgentConfig.def_readwrite("tick_config_", &PyAgentConfig::tick_config_);
      PB__PyAgentConfig.def_readwrite("stderr_fd_", &PyAgentConfig::stderr_fd_);
      PB__PyAgentConfig.def_readwrite("mlockall_", &PyAgentConfig::mlockall_);
      PB__PyAgentConfig.def_readwrite("pydata", &PyAgentConfig::pydata);
    }

    py::class_<PyTask> PB__PyTask(PB__ghost, "PyTask"); {
      PB__PyTask.def(py::init<ghost::Gtid, ghost_sw_info>());
      PB__PyTask.def_readwrite("gtid", &PyTask::gtid);
      PB__PyTask.def_readonly("status_word", &PyTask::status_word);
      PB__PyTask.def_readwrite("seqnum", &PyTask::seqnum);
      PB__PyTask.def("Advance", &PyTask::Advance);
      PB__PyTask.def_readwrite("pydata", &PyTask::pydata);
    }

    py::class_<PyWrapAgentConfig, AgentConfig> PB__PyWrapAgentConfig(PB__ghost, "PyWrapAgentConfig"); {
      PB__PyWrapAgentConfig.def(py::init<ghost::PyAgentConfig, MAKEFULLAGENT_T>(), py::keep_alive<1, 3>());
      //PB__PyWrapAgentConfig.def_readwrite("topology_", &PyWrapAgentConfig::topology_);
      PB__PyWrapAgentConfig.def_readwrite("cpus_", &PyWrapAgentConfig::cpus_);
      PB__PyWrapAgentConfig.def_readwrite("enclave_fd_", &PyWrapAgentConfig::enclave_fd_);
      PB__PyWrapAgentConfig.def_readwrite("tick_config_", &PyWrapAgentConfig::tick_config_);
      PB__PyWrapAgentConfig.def_readwrite("stderr_fd_", &PyWrapAgentConfig::stderr_fd_);
      PB__PyWrapAgentConfig.def_readwrite("mlockall_", &PyWrapAgentConfig::mlockall_);
      PB__PyWrapAgentConfig.def("make", &PyWrapAgentConfig::make);
    }

    py::class_<RunRequest> PB__RunRequest(PB__ghost, "RunRequest"); {
      PB__RunRequest.def(py::init<>());
      PB__RunRequest.def("Init", &RunRequest::Init);
      PB__RunRequest.def("Open", &RunRequest::Open);
      PB__RunRequest.def("OpenUnschedule", &RunRequest::OpenUnschedule);
      PB__RunRequest.def("Abort", &RunRequest::Abort);
      PB__RunRequest.def("LocalYield", &RunRequest::LocalYield, py::call_guard<py::gil_scoped_release>());
      PB__RunRequest.def("Ping", &RunRequest::Ping);
      PB__RunRequest.def("Commit", &RunRequest::Commit, py::call_guard<py::gil_scoped_release>());
      PB__RunRequest.def("Submit", &RunRequest::Submit);
      PB__RunRequest.def("cpu", &RunRequest::cpu);
      PB__RunRequest.def("state", &RunRequest::state);
      PB__RunRequest.def("open", &RunRequest::open);
      PB__RunRequest.def("committed", &RunRequest::committed);
      PB__RunRequest.def("failed", &RunRequest::failed);
      PB__RunRequest.def("succeeded", &RunRequest::succeeded);
      PB__RunRequest.def("commit_time", &RunRequest::commit_time);
      PB__RunRequest.def("sync_group_owner_get", &RunRequest::sync_group_owner_get);
      PB__RunRequest.def("sync_group_owner_set", &RunRequest::sync_group_owner_set);
      PB__RunRequest.def("sync_group_owned", &RunRequest::sync_group_owned);
      PB__RunRequest.def("agent_barrier", &RunRequest::agent_barrier);
      PB__RunRequest.def("target", &RunRequest::target);
      PB__RunRequest.def("target_barrier", &RunRequest::target_barrier);
      PB__RunRequest.def("commit_flags", &RunRequest::commit_flags);
      PB__RunRequest.def("run_flags", &RunRequest::run_flags);
      PB__RunRequest.def("allow_txn_target_on_cpu", &RunRequest::allow_txn_target_on_cpu);
      PB__RunRequest.def("cpu_seqnum", &RunRequest::cpu_seqnum);
      PB__RunRequest.def("txn", &RunRequest::txn);
      PB__RunRequest.def_static("StateToString", &RunRequest::StateToString);
    }

    py::class_<RunRequestOptions> PB__RunRequestOptions(PB__ghost, "RunRequestOptions"); {
      PB__RunRequestOptions.def(py::init<>());
      PB__RunRequestOptions.def_readwrite("target", &RunRequestOptions::target);
      PB__RunRequestOptions.def_readwrite("target_barrier", &RunRequestOptions::target_barrier);
      PB__RunRequestOptions.def_readwrite("agent_barrier", &RunRequestOptions::agent_barrier);
      PB__RunRequestOptions.def_readwrite("commit_flags", &RunRequestOptions::commit_flags);
      PB__RunRequestOptions.def_readwrite("run_flags", &RunRequestOptions::run_flags);
      PB__RunRequestOptions.def_readwrite("sync_group_owner", &RunRequestOptions::sync_group_owner);
      PB__RunRequestOptions.def_readwrite("allow_txn_target_on_cpu", &RunRequestOptions::allow_txn_target_on_cpu);
    }

    py::class_<Scheduler, TrPB__Scheduler> PB__Scheduler(PB__ghost, "Scheduler"); {
      PB__Scheduler.def(py::init<ghost::Enclave*, ghost::CpuList>());
      PB__Scheduler.def_readonly_static("kDumpStateEmptyRQ", &Scheduler::kDumpStateEmptyRQ);
      PB__Scheduler.def_readonly_static("kDumpAllTasks", &Scheduler::kDumpAllTasks);
      PB__Scheduler.def("EnclaveReady", &Scheduler::EnclaveReady);
      PB__Scheduler.def("DiscoverTasks", &Scheduler::DiscoverTasks);
      PB__Scheduler.def("GetDefaultChannel", &Scheduler::GetDefaultChannel);
//      PB__Scheduler.def("SchedTopology", &Scheduler::SchedTopology);
      PB__Scheduler.def("DumpState", &Scheduler::DumpState);
      PB__Scheduler.def("cpus", &PuPB__Scheduler::cpus, py::return_value_policy::reference);
    }

    py::class_<TaskAllocator<PyTask>, TrPB__TaskAllocator_PyTask_, std::shared_ptr<TaskAllocator<PyTask>>> PB__TaskAllocator_PyTask_(PB__ghost, "TaskAllocator_PyTask_"); {
      PB__TaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid>(&TaskAllocator<PyTask>::GetTask), py::call_guard<py::gil_scoped_release>());
      PB__TaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid, ghost_sw_info>(&TaskAllocator<PyTask>::GetTask), py::call_guard<py::gil_scoped_release>());
      PB__TaskAllocator_PyTask_.def("FreeTask", &TaskAllocator<PyTask>::FreeTask, py::call_guard<py::gil_scoped_release>());
      PB__TaskAllocator_PyTask_.def("ForEachTask", &TaskAllocator<PyTask>::ForEachTask, py::call_guard<py::gil_scoped_release>());
    }

    py::class_<SingleThreadMallocTaskAllocator<PyTask>, TaskAllocator<PyTask>, TrPB__SingleThreadMallocTaskAllocator_PyTask_, std::shared_ptr<SingleThreadMallocTaskAllocator<PyTask>>> PB__SingleThreadMallocTaskAllocator_PyTask_(PB__ghost, "SingleThreadMallocTaskAllocator_PyTask_"); {
      PB__SingleThreadMallocTaskAllocator_PyTask_.def("ForEachTask", py::overload_cast<ghost::TaskAllocator<PyTask>::TaskCallbackFunc>(&SingleThreadMallocTaskAllocator<PyTask>::ForEachTask));
      PB__SingleThreadMallocTaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid>(&SingleThreadMallocTaskAllocator<PyTask>::GetTask));
      PB__SingleThreadMallocTaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid, ghost_sw_info>(&SingleThreadMallocTaskAllocator<PyTask>::GetTask));
      PB__SingleThreadMallocTaskAllocator_PyTask_.def("FreeTask", &SingleThreadMallocTaskAllocator<PyTask>::FreeTask);
      PB__SingleThreadMallocTaskAllocator_PyTask_.def("ForEachTask", py::overload_cast<typename TaskAllocator<PyTask>::TaskCallbackFunc>(&SingleThreadMallocTaskAllocator<PyTask>::ForEachTask));
    }

    py::class_<StatusWord, TrPB__StatusWord> PB__StatusWord(PB__ghost, "StatusWord"); {
      PB__StatusWord.def("Free", &StatusWord::Free);
      PB__StatusWord.def("empty", &StatusWord::empty);
      PB__StatusWord.def_static("NullBarrierToken", &StatusWord::NullBarrierToken);
      PB__StatusWord.def("barrier", &StatusWord::barrier);
      PB__StatusWord.def("switch_time", &StatusWord::switch_time);
      PB__StatusWord.def("runtime", &StatusWord::runtime);
      PB__StatusWord.def("stale", &StatusWord::stale);
      PB__StatusWord.def("in_use", &StatusWord::in_use);
      PB__StatusWord.def("can_free", &StatusWord::can_free);
      PB__StatusWord.def("on_cpu", &StatusWord::on_cpu);
      PB__StatusWord.def("cpu_avail", &StatusWord::cpu_avail);
      PB__StatusWord.def("runnable", &StatusWord::runnable);
      PB__StatusWord.def("boosted_priority", &StatusWord::boosted_priority);
      PB__StatusWord.def("id", &StatusWord::id);
      PB__StatusWord.def("owner", &StatusWord::owner);
      PB__StatusWord.def("sw_info", &StatusWord::sw_info);
      PB__StatusWord.def("sw", &StatusWord::sw);
      py::class_<StatusWord::AgentSW> PB__AgentSW(PB__StatusWord, "AgentSW"); {
      }

    }

    py::class_<StatusWordTable> PB__StatusWordTable(PB__ghost, "StatusWordTable"); {
      PB__StatusWordTable.def("id", &StatusWordTable::id);
      PB__StatusWordTable.def("get", &StatusWordTable::get);
      PB__StatusWordTable.def("ForEachTaskStatusWord", &StatusWordTable::ForEachTaskStatusWord);
    }

    py::class_<Task<LocalStatusWord>> PB__Task_LocalStatusWord_(PB__ghost, "Task_LocalStatusWord_"); {
      PB__Task_LocalStatusWord_.def(py::init<ghost::Gtid, ghost_sw_info>());
      PB__Task_LocalStatusWord_.def_readwrite("gtid", &Task<LocalStatusWord>::gtid);
      PB__Task_LocalStatusWord_.def_readonly("status_word", &Task<LocalStatusWord>::status_word);
      PB__Task_LocalStatusWord_.def_readwrite("seqnum", &Task<LocalStatusWord>::seqnum);
      PB__Task_LocalStatusWord_.def("Advance", &Task<LocalStatusWord>::Advance);
    }

    py::class_<ThreadSafeMallocTaskAllocator<PyTask>, SingleThreadMallocTaskAllocator<PyTask>, TrPB__ThreadSafeMallocTaskAllocator_PyTask_, std::shared_ptr<ThreadSafeMallocTaskAllocator<PyTask>>> PB__ThreadSafeMallocTaskAllocator_PyTask_(PB__ghost, "ThreadSafeMallocTaskAllocator_PyTask_"); {
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def(py::init<>());
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def("ForEachTask", py::overload_cast<ghost::TaskAllocator<PyTask>::TaskCallbackFunc>(&ThreadSafeMallocTaskAllocator<PyTask>::ForEachTask));
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid>(&ThreadSafeMallocTaskAllocator<PyTask>::GetTask));
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def("GetTask", py::overload_cast<ghost::Gtid, ghost_sw_info>(&ThreadSafeMallocTaskAllocator<PyTask>::GetTask));
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def("FreeTask", &ThreadSafeMallocTaskAllocator<PyTask>::FreeTask);
      PB__ThreadSafeMallocTaskAllocator_PyTask_.def("ForEachTask", py::overload_cast<typename TaskAllocator<PyTask>::TaskCallbackFunc>(&ThreadSafeMallocTaskAllocator<PyTask>::ForEachTask));
    }

    py::class_<Topology> PB__Topology(PB__ghost, "Topology"); {
//      PB__Topology.def_readonly_static("kNumTestCpus", &Topology::kNumTestCpus);
//      PB__Topology.def("Export", &Topology::Export);
//      PB__Topology.def("EmptyCpuList", &Topology::EmptyCpuList);
//      PB__Topology.def("ToCpuList", py::overload_cast<std::vector<int> const&>(&Topology::ToCpuList, py::const_));
//      PB__Topology.def("ToCpuList", py::overload_cast<std::vector<Cpu> const&>(&Topology::ToCpuList, py::const_));
//      PB__Topology.def("ToCpuList", py::overload_cast<cpu_set_t const&>(&Topology::ToCpuList, py::const_));
//      PB__Topology.def_static("ToCpuSet", &Topology::ToCpuSet);
//      PB__Topology.def("num_cpus", &Topology::num_cpus);
//      PB__Topology.def("smt_count", &Topology::smt_count);
//      PB__Topology.def("num_numa_nodes", &Topology::num_numa_nodes);
//      PB__Topology.def("cpu", &Topology::cpu);
//      PB__Topology.def("all_cpus", &Topology::all_cpus);
//      PB__Topology.def("Core", &Topology::Core);
//      PB__Topology.def("Cores", &Topology::Cores);
//      PB__Topology.def("all_cores", &Topology::all_cores);
//      PB__Topology.def("highest_node_idx", &Topology::highest_node_idx);
//      PB__Topology.def("ParseCpuStr", &Topology::ParseCpuStr);
//      PB__Topology.def("CpusOnNode", &Topology::CpusOnNode);
    }

    py::class_<WrapFullAgent> PB__WrapFullAgent(PB__ghost, "WrapFullAgent"); {
      PB__WrapFullAgent.def(py::init<ghost::PyWrapAgentConfig>());
      //PB__WrapFullAgent.def_readwrite("py_agent", &WrapFullAgent::py_agent);
      PB__WrapFullAgent.def("RpcHandler", &WrapFullAgent::RpcHandler);
    }

    py::class_<BasicDispatchScheduler<PyTask>, Scheduler, TrPB__BasicDispatchScheduler_PyTask_> PB__BasicDispatchScheduler_PyTask_(PB__ghost, "BasicDispatchScheduler_PyTask_"); {
      PB__BasicDispatchScheduler_PyTask_.def(py::init<ghost::Enclave*, ghost::CpuList, std::shared_ptr<TaskAllocator<PyTask>>>());
      PB__BasicDispatchScheduler_PyTask_.def_readonly_static("kDumpStateEmptyRQ", &BasicDispatchScheduler<PyTask>::kDumpStateEmptyRQ);
      PB__BasicDispatchScheduler_PyTask_.def_readonly_static("kDumpAllTasks", &BasicDispatchScheduler<PyTask>::kDumpAllTasks);
      PB__BasicDispatchScheduler_PyTask_.def("EnclaveReady", &BasicDispatchScheduler<PyTask>::EnclaveReady);
      PB__BasicDispatchScheduler_PyTask_.def("GetDefaultChannel", &BasicDispatchScheduler<PyTask>::GetDefaultChannel);
      //PB__BasicDispatchScheduler_PyTask_.def("SchedTopology", &BasicDispatchScheduler<PyTask>::SchedTopology);
      PB__BasicDispatchScheduler_PyTask_.def("DumpState", &BasicDispatchScheduler<PyTask>::DumpState);
      PB__BasicDispatchScheduler_PyTask_.def("DispatchMessage", &BasicDispatchScheduler<PyTask>::DispatchMessage, py::call_guard<py::gil_scoped_release>());
      PB__BasicDispatchScheduler_PyTask_.def("DiscoverTasks", &BasicDispatchScheduler<PyTask>::DiscoverTasks);

      PB__BasicDispatchScheduler_PyTask_.def("TaskNew", &PuPB__BasicDispatchScheduler_PyTask_::TaskNew);
      PB__BasicDispatchScheduler_PyTask_.def("TaskRunnable", &PuPB__BasicDispatchScheduler_PyTask_::TaskRunnable);
      PB__BasicDispatchScheduler_PyTask_.def("TaskDead", &PuPB__BasicDispatchScheduler_PyTask_::TaskDead);
      PB__BasicDispatchScheduler_PyTask_.def("TaskDeparted", &PuPB__BasicDispatchScheduler_PyTask_::TaskDeparted);
      PB__BasicDispatchScheduler_PyTask_.def("TaskYield", &PuPB__BasicDispatchScheduler_PyTask_::TaskYield);
      PB__BasicDispatchScheduler_PyTask_.def("TaskBlocked", &PuPB__BasicDispatchScheduler_PyTask_::TaskBlocked);
      PB__BasicDispatchScheduler_PyTask_.def("TaskPreempted", &PuPB__BasicDispatchScheduler_PyTask_::TaskPreempted);
      PB__BasicDispatchScheduler_PyTask_.def("enclave", &PuPB__BasicDispatchScheduler_PyTask_::enclave, py::return_value_policy::reference);
      PB__BasicDispatchScheduler_PyTask_.def("cpus", &PuPB__BasicDispatchScheduler_PyTask_::cpus, py::return_value_policy::reference);
      PB__BasicDispatchScheduler_PyTask_.def("allocator", &PuPB__BasicDispatchScheduler_PyTask_::allocator, py::return_value_policy::reference);
    }

    py::class_<LocalStatusWord, StatusWord, TrPB__LocalStatusWord> PB__LocalStatusWord(PB__ghost, "LocalStatusWord"); {
      PB__LocalStatusWord.def(py::init<>());
      PB__LocalStatusWord.def(py::init<ghost::Gtid, ghost_sw_info>());
      PB__LocalStatusWord.def(py::init<StatusWord::AgentSW>());
      //PB__LocalStatusWord.def(py::init<ghost::LocalStatusWord&&>());
      PB__LocalStatusWord.def("empty", &LocalStatusWord::empty);
      PB__LocalStatusWord.def_static("NullBarrierToken", &LocalStatusWord::NullBarrierToken);
      PB__LocalStatusWord.def("barrier", &LocalStatusWord::barrier);
      PB__LocalStatusWord.def("switch_time", &LocalStatusWord::switch_time);
      PB__LocalStatusWord.def("runtime", &LocalStatusWord::runtime);
      PB__LocalStatusWord.def("stale", &LocalStatusWord::stale);
      PB__LocalStatusWord.def("in_use", &LocalStatusWord::in_use);
      PB__LocalStatusWord.def("can_free", &LocalStatusWord::can_free);
      PB__LocalStatusWord.def("on_cpu", &LocalStatusWord::on_cpu);
      PB__LocalStatusWord.def("cpu_avail", &LocalStatusWord::cpu_avail);
      PB__LocalStatusWord.def("runnable", &LocalStatusWord::runnable);
      PB__LocalStatusWord.def("boosted_priority", &LocalStatusWord::boosted_priority);
      PB__LocalStatusWord.def("id", &LocalStatusWord::id);
      PB__LocalStatusWord.def("owner", &LocalStatusWord::owner);
      PB__LocalStatusWord.def("sw_info", &LocalStatusWord::sw_info);
      PB__LocalStatusWord.def("sw", &LocalStatusWord::sw);
      //PB__LocalStatusWord.def("operator_", py::overload_cast<ghost::LocalStatusWord&&>(&LocalStatusWord::operator=));
      PB__LocalStatusWord.def("Free", &LocalStatusWord::Free);
    }

    py::class_<LocalStatusWordTable, StatusWordTable> PB__LocalStatusWordTable(PB__ghost, "LocalStatusWordTable"); {
      PB__LocalStatusWordTable.def(py::init<int, int, int>());
      PB__LocalStatusWordTable.def("id", &LocalStatusWordTable::id);
      PB__LocalStatusWordTable.def("get", &LocalStatusWordTable::get);
      PB__LocalStatusWordTable.def("ForEachTaskStatusWord", &LocalStatusWordTable::ForEachTaskStatusWord);
    }

//    PB__ghost.def("MachineTopology", &MachineTopology);
//    PB__ghost.def("UpdateTestTopology", &UpdateTestTopology);
//    PB__ghost.def("TestTopology", &TestTopology);
//    PB__ghost.def("UpdateCustomTopology", &UpdateCustomTopology);
//    PB__ghost.def("CustomTopology", &CustomTopology);
    PB__ghost.def("GetTID", &GetTID);
    PB__ghost.def("Exit", &Exit);
    PB__ghost.def("GetFileSize", &GetFileSize);
    PB__ghost.def("PrintBacktrace", &PrintBacktrace);
    PB__ghost.def("MonotonicNow", &MonotonicNow);
    PB__ghost.def("GetTID", &GetTID);
    PB__ghost.def("GetGtid", &GetGtid);
    PB__ghost.def("Pause", &Pause, py::call_guard<py::gil_scoped_release>());
    PB__ghost.def("Peek", &Peek);
    PB__ghost.def("Consume", &Consume);
    PB__ghost.def("verbose", &verbose);
    PB__ghost.def("set_verbose", &set_verbose);
    PB__ghost.def("SchedTaskEnterGhost", &SchedTaskEnterGhost);
    PB__ghost.def("SchedAgentEnterGhost", &SchedAgentEnterGhost);
    PB__ghost.def("GetTopoCpuList", &GetTopoCpuList);
    PB__ghost.def("getConfig", &getConfig);
    PB__ghost.def("SingleCpu", &SingleCpu);
    PB__ghost.def("GetCpu", &GetCpu);


    PB__ghost.attr("GHOST_MAX_QUEUE_ELEMS") = py::int_(GHOST_MAX_QUEUE_ELEMS);
    PB__ghost.attr("COMMIT_AT_TXN_COMMIT") = py::int_(COMMIT_AT_TXN_COMMIT);
    PB__ghost.attr("RTLA_ON_IDLE") = py::int_(RTLA_ON_IDLE);

    py::class_<ghost_msg_payload_task_new>(PB__ghost, "ghost_msg_payload_task_new")
      .def_readwrite("gtid", &ghost_msg_payload_task_new::gtid)
      .def_readwrite("runtime", &ghost_msg_payload_task_new::runtime)
      .def_readwrite("runnable", &ghost_msg_payload_task_new::runnable);
    PB__ghost.def("cast_payload_new", &cast_payload_new, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_preempt>(PB__ghost, "ghost_msg_payload_task_preempt")
      .def_readwrite("gtid", &ghost_msg_payload_task_preempt::gtid)
      .def_readwrite("runtime", &ghost_msg_payload_task_preempt::runtime)
      .def_readwrite("cpu_seqnum", &ghost_msg_payload_task_preempt::cpu_seqnum)
      .def_readwrite("agent_data", &ghost_msg_payload_task_preempt::agent_data)
      .def_readwrite("cpu", &ghost_msg_payload_task_preempt::cpu)
      .def_readwrite("from_switchto", &ghost_msg_payload_task_preempt::from_switchto)
      .def_readwrite("was_latched", &ghost_msg_payload_task_preempt::was_latched);
    PB__ghost.def("cast_payload_preempt", &cast_payload_preempt, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_yield>(PB__ghost, "ghost_msg_payload_task_yield")
      .def_readwrite("gtid", &ghost_msg_payload_task_yield::gtid)
      .def_readwrite("runtime", &ghost_msg_payload_task_yield::runtime)
      .def_readwrite("cpu_seqnum", &ghost_msg_payload_task_yield::cpu_seqnum)
      .def_readwrite("agent_data", &ghost_msg_payload_task_yield::agent_data)
      .def_readwrite("cpu", &ghost_msg_payload_task_yield::cpu)
      .def_readwrite("from_switchto", &ghost_msg_payload_task_yield::from_switchto);
    PB__ghost.def("cast_payload_yield", &cast_payload_yield, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_blocked>(PB__ghost, "ghost_msg_payload_task_blocked")
      .def_readwrite("gtid", &ghost_msg_payload_task_blocked::gtid)
      .def_readwrite("runtime", &ghost_msg_payload_task_blocked::runtime)
      .def_readwrite("cpu_seqnum", &ghost_msg_payload_task_blocked::cpu_seqnum)
      .def_readwrite("cpu", &ghost_msg_payload_task_blocked::cpu)
      .def_readwrite("from_switchto", &ghost_msg_payload_task_blocked::from_switchto);
    PB__ghost.def("cast_payload_blocked", &cast_payload_blocked, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_dead>(PB__ghost, "ghost_msg_payload_task_dead")
      .def_readwrite("gtid", &ghost_msg_payload_task_dead::gtid);
    PB__ghost.def("cast_payload_dead", &cast_payload_dead, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_departed>(PB__ghost, "ghost_msg_payload_task_departed")
      .def_readwrite("gtid", &ghost_msg_payload_task_departed::gtid)
      .def_readwrite("cpu_seqnum", &ghost_msg_payload_task_departed::cpu_seqnum)
      .def_readwrite("cpu", &ghost_msg_payload_task_departed::cpu)
      .def_readwrite("from_switchto", &ghost_msg_payload_task_departed::from_switchto)
      .def_readwrite("was_current", &ghost_msg_payload_task_departed::was_current);
    PB__ghost.def("cast_payload_departed", &cast_payload_departed, py::return_value_policy::reference);

    py::class_<ghost_msg_payload_task_wakeup>(PB__ghost, "ghost_msg_payload_task_wakeup")
      .def_readwrite("gtid", &ghost_msg_payload_task_wakeup::gtid)
      .def_readwrite("agent_data", &ghost_msg_payload_task_wakeup::agent_data)
      .def_readwrite("deferrable", &ghost_msg_payload_task_wakeup::deferrable)
      .def_readwrite("last_ran_cpu", &ghost_msg_payload_task_wakeup::last_ran_cpu)
      .def_readwrite("wake_up_cpu", &ghost_msg_payload_task_wakeup::wake_up_cpu)
      .def_readwrite("waker_cpu", &ghost_msg_payload_task_wakeup::waker_cpu);
    PB__ghost.def("cast_payload_wakeup", &cast_payload_wakeup, py::return_value_policy::reference);
  }

}
