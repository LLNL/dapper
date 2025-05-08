from __future__ import annotations

import requests
import concurrent.futures
import numpy as np
import scipy as sp
import matplotlib.pyplot as plt

from pathlib import Path
from tqdm.auto import tqdm
from typing import Callable

#Custom modules
import MyCustomModuleV1

from MyLibrary1 import my_func
from MyLibrary2.submodule import MyClass
from ..MyRelativeLib import rel_func as a_func
from MyConstants import ConstantA as NewName, ConstantB as NewName2



def test_func(arg:int) -> int:
	return arg

def main() -> None:
	print('Hello World')

if __name__ == "__main__":
	main()