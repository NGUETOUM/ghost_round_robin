// Python-specific additional functions to work with the ghOSt library, by Victor Miquel

#pragma once

#include <pybind11/pybind11.h>
namespace py = pybind11;

#include "lib/agent.h"
#include "lib/scheduler.h"
#include "kernel/ghost_uapi.h"
#include "lib/enclave.h"

namespace ghost {

struct PyTask : public Task<LocalStatusWord> {
  PyTask(Gtid fifo_task_gtid, ghost_sw_info sw_info)
    : Task<>(fifo_task_gtid, sw_info), pydata(pybind11::none()) {}
  ~PyTask() override {}

  py::object pydata;
};

class PyAgentConfig : public AgentConfig {
 public:
  py::object pydata;
};

class PyGlobalAgentConfig : public PyAgentConfig{
public:
  PyGlobalAgentConfig(int global_cpu, int64_t preemption_time_slice)
  : global_cpu(global_cpu), preemption_time_slice(preemption_time_slice){}

  int global_cpu;
  int64_t preemption_time_slice;
};

#define FULLAGENT_T py::object//FullAgent<LocalEnclave, PyAgentConfig>
#define MAKEFULLAGENT_T std::function < FULLAGENT_T (PyAgentConfig) >



class PyWrapAgentConfig : public AgentConfig {
  PyAgentConfig config;
  MAKEFULLAGENT_T mfa;

 public:
  PyWrapAgentConfig(PyAgentConfig conf, MAKEFULLAGENT_T cons);

  FULLAGENT_T make();
};



class WrapFullAgent {
 public:
  py::object py_agent;

  WrapFullAgent(PyWrapAgentConfig wconfig);

  void RpcHandler(int64_t req, const AgentRpcArgs& args, AgentRpcResponse& response);
};


extern template struct Task<LocalStatusWord>;
extern template class TaskAllocator<PyTask>;
extern template class SingleThreadMallocTaskAllocator<PyTask>;
extern template class ThreadSafeMallocTaskAllocator<PyTask>;
extern template class BasicDispatchScheduler<PyTask>;
extern template class AgentProcess<WrapFullAgent, PyWrapAgentConfig>;
extern template class FullAgent<LocalEnclave, PyAgentConfig>;

CpuList GetTopoCpuList();

CpuList GetEmptyCpuList();

PyAgentConfig getConfig();


CpuList SingleCpu(Cpu const&);

Cpu GetCpu(int cpu);

bool txnReqEqualToGHOST_TXN_COMPLETE(RunRequest req);

const ghost_msg_payload_task_new* cast_payload_new(void* payload);
const ghost_msg_payload_task_wakeup* cast_payload_wakeup(void* payload);
const ghost_msg_payload_task_departed* cast_payload_departed(void* payload);
const ghost_msg_payload_task_dead* cast_payload_dead(void* payload);
const ghost_msg_payload_task_yield* cast_payload_yield(void* payload);
const ghost_msg_payload_task_blocked* cast_payload_blocked(void* payload);
const ghost_msg_payload_task_preempt* cast_payload_preempt(void* payload);

} // namespace ghost
