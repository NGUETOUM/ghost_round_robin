// Python-specific additional functions and classes to work with the ghOSt library, by Victor Miquel

#include "python_interface.hpp"

#include <pybind11/pybind11.h>
namespace py = pybind11;

#include "lib/agent.h"
#include "kernel/ghost_uapi.h"
#include "lib/enclave.h"

namespace ghost {

PyWrapAgentConfig::PyWrapAgentConfig(PyAgentConfig confif, MAKEFULLAGENT_T cons) : config(confif), mfa(cons) {}

FULLAGENT_T PyWrapAgentConfig::make() {
  py::gil_scoped_acquire acquire;
  return mfa(config);
}

WrapFullAgent::WrapFullAgent(PyWrapAgentConfig wconfig) : py_agent(wconfig.make()) {
}

void WrapFullAgent::RpcHandler(int64_t req, const AgentRpcArgs& args, AgentRpcResponse& response) {
  py_agent.attr("RpcHandler")(req, args, response);
}

template struct Task<LocalStatusWord>;
template class TaskAllocator<PyTask>;
template class SingleThreadMallocTaskAllocator<PyTask>;
template class ThreadSafeMallocTaskAllocator<PyTask>;
template class BasicDispatchScheduler<PyTask>;
template class AgentProcess<WrapFullAgent, PyWrapAgentConfig>;
template class FullAgent<LocalEnclave, PyAgentConfig>;

CpuList GetTopoCpuList() {
  return MachineTopology()->all_cores();
}

CpuList GetEmptyCpuList(){
  return MachineTopology()->EmptyCpuList();
}

PyAgentConfig getConfig() {
  PyAgentConfig config;
  Topology* topology = MachineTopology();

  config.topology_ = topology;
  config.cpus_ = topology->all_cpus();

  return config;
}

PyGlobalAgentConfig getGlobalConfig() {
  int global_cpu = GetCpu(1).id();
  int64_t preemption_time_slice = absl::ToInt64Seconds(absl::Milliseconds(50));
  PyGlobalAgentConfig config(global_cpu, preemption_time_slice);
  Topology* topology = MachineTopology();

  config.topology_ = topology;
  config.cpus_ = topology->all_cpus();
  return config;
}


CpuList SingleCpu(Cpu const& cpu) {
  return MachineTopology()->ToCpuList({cpu});
}

Cpu GetCpu(int cpu) {
  return MachineTopology()->cpu(cpu);
}

bool txnReqEqualToGHOST_TXN_COMPLETE(RunRequest req){
  if(req.state() == GHOST_TXN_COMPLETE){
    return true;
  }else{
    return false;
  }
}

const ghost_msg_payload_task_new* cast_payload_new(void* payload) {
  return static_cast<const ghost_msg_payload_task_new*>(payload);
}

const ghost_msg_payload_task_wakeup* cast_payload_wakeup(void* payload) {
  return static_cast<const ghost_msg_payload_task_wakeup*>(payload);
}

const ghost_msg_payload_task_departed* cast_payload_departed(void* payload) {
  return static_cast<const ghost_msg_payload_task_departed*>(payload);
}

const ghost_msg_payload_task_dead* cast_payload_dead(void* payload) {
  return static_cast<const ghost_msg_payload_task_dead*>(payload);
}

const ghost_msg_payload_task_yield* cast_payload_yield(void* payload) {
  return static_cast<const ghost_msg_payload_task_yield*>(payload);
}

const ghost_msg_payload_task_blocked* cast_payload_blocked(void* payload) {
  return static_cast<const ghost_msg_payload_task_blocked*>(payload);
}

const ghost_msg_payload_task_preempt* cast_payload_preempt(void* payload) {
  return static_cast<const ghost_msg_payload_task_preempt*>(payload);
}

} // namespace ghost
