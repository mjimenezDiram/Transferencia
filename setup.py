from distutils.core import setup
import py2exe

setup(
    console = [
        {
            "script": "GeneralTransfer.py",           ### Main Python script    
            "icon_resources": [(0, "PQicon.ico")]     ### Icon to embed into the PE file.
        }
    ],
)
