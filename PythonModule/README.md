# Python Bindings for the ghOSt library

This bindings were produced by manually editing the result of the `BindingTool`.
Note that anything related to the `Topology` class is not binded. Additional functions have been added to work around that.

Please refer to PythonFifo for usage, especially on how startup is done.
Except for scheduler startup and `Topology`, pretty much everything behaves as the C++ library.

`python_interface.*` provide the additional classes and functions that allow working with the ghOSt library in Python.
