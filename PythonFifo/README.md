# Python Scheduler Example: FIFO

This is a working example of a Python scheduler.
It is intented to replicate the scheduling decisions made by the C++ per-cpu fifo scheduler from the `ghost-userspace` provided by Google.

Run `bazel run python_fifo` as root from the `ghost-userspace` directory to run. (refer to the repository's root `README.md` for instalation)
Note that currently this scheduler does not shut down properly. Running `for k in /sys/fs/ghost/enclave_*/ctl; do echo destroy > $k; done` is therefore mandatory to clean-up after each run.
