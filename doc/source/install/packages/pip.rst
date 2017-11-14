Installation with pip
=====================

When installing Pyrocko through ``pip`` the dependencies should be 

Install from PiPython
---------------------

Tagged builds are available for download from https://pypi.python.org/.

.. note :: 

    Pyrockos' build depends on Python and Numpy developer source files, please install those through your system mananger. Please see :doc:`../system/index` for more information.

.. warning ::
    
    Dependency for **Qt** is not resolved, please use your system package manager to install _PyQt4_ or _PyQt5_!
    For more information see :doc:`../system/index`

.. code-block:: bash
    :caption: Source packages for Pyrocko are available on `pypi.python.org <https://pypi.python.org>`_

    # Install build requirements
    sudo apt-get install python3-dev python3-numpy
    sudo pip3 install pyrocko

    # Install requirements
    sudo pip3 install numpy>=1.8 scipy pyyaml matplotlib progressbar2 future jinja2 requests PyOpenGL


Pyrocko Pip User Install
^^^^^^^^^^^^^^^^^^^^^^^^

If you want to install pyrocko in a user environment without root access you can use PIP to manage the installation as well:

.. code-block:: bash
    :caption: ``pip`` allows to install into user's environments - **dangerous for system integrity**

    # We assume the build requirements are already installed, see above
    pip3 install pyrocko

    # Install requirements
    pip3 install numpy>=1.8 scipy pyyaml matplotlib progressbar2 future jinja2 requests PyOpenGL




Install from Github
-------------------

If you want to install the latest ``master`` from Github you can use ``pip`` to install directly from the repository:

.. code-block:: bash
    :caption: We install straight from GitHub

    sudo pip3 install git+https://github.com/pyrocko/pyrocko.git