from datetime import datetime, timedelta

import numpy as np
import pytest

np_index_exp_params = [
    np.index_exp[:],
    np.index_exp[...],
    np.index_exp[0],
    np.index_exp[1],
    np.index_exp[-1],
    np.index_exp[-2],
    np.index_exp[0, :],
    np.index_exp[1, :],
    np.index_exp[-1, :],
    np.index_exp[-2, :],
    np.index_exp[0, ...],
    np.index_exp[1, ...],
    np.index_exp[-1, ...],
    np.index_exp[-2, ...],
    np.index_exp[0, 0],
    np.index_exp[1, 0],
    np.index_exp[-1, 0],
    np.index_exp[-2, 0],
    np.index_exp[0, 1],
    np.index_exp[1, 1],
    np.index_exp[-1, 1],
    np.index_exp[-2, 1],
    np.index_exp[0, -1],
    np.index_exp[1, -1],
    np.index_exp[-1, -1],
    np.index_exp[-2, -1],
    np.index_exp[0, -2],
    np.index_exp[1, -2],
    np.index_exp[-1, -2],
    np.index_exp[-2, -2],
    np.index_exp[0, 0, :],
    np.index_exp[1, 0, :],
    np.index_exp[-1, 0, :],
    np.index_exp[-2, 0, :],
    np.index_exp[0, 1, :],
    np.index_exp[1, 1, :],
    np.index_exp[-1, 1, :],
    np.index_exp[-2, 1, :],
    np.index_exp[0, -1, :],
    np.index_exp[1, -1, :],
    np.index_exp[-1, -1, :],
    np.index_exp[-2, -1, :],
    np.index_exp[0, -2, :],
    np.index_exp[1, -2, :],
    np.index_exp[-1, -2, :],
    np.index_exp[-2, -2, :],
    np.index_exp[0, 0, ...],
    np.index_exp[1, 0, ...],
    np.index_exp[-1, 0, ...],
    np.index_exp[-2, 0, ...],
    np.index_exp[0, 1, ...],
    np.index_exp[1, 1, ...],
    np.index_exp[-1, 1, ...],
    np.index_exp[-2, 1, ...],
    np.index_exp[0, -1, ...],
    np.index_exp[1, -1, ...],
    np.index_exp[-1, -1, ...],
    np.index_exp[-2, -1, ...],
    np.index_exp[0, -2, ...],
    np.index_exp[1, -2, ...],
    np.index_exp[-1, -2, ...],
    np.index_exp[-2, -2, ...],
    np.index_exp[0, 0, 0],
    np.index_exp[1, 0, 0],
    np.index_exp[-1, 0, 0],
    np.index_exp[-2, 0, 0],
    np.index_exp[0, 1, 0],
    np.index_exp[1, 1, 0],
    np.index_exp[-1, 1, 0],
    np.index_exp[-2, 1, 0],
    np.index_exp[0, -1, 0],
    np.index_exp[1, -1, 0],
    np.index_exp[-1, -1, 0],
    np.index_exp[-2, -1, 0],
    np.index_exp[0, -2, 0],
    np.index_exp[1, -2, 0],
    np.index_exp[-1, -2, 0],
    np.index_exp[-2, -2, 0],
    np.index_exp[0, 0, 1],
    np.index_exp[1, 0, 1],
    np.index_exp[-1, 0, 1],
    np.index_exp[-2, 0, 1],
    np.index_exp[0, 1, 1],
    np.index_exp[1, 1, 1],
    np.index_exp[-1, 1, 1],
    np.index_exp[-2, 1, 1],
    np.index_exp[0, -1, 1],
    np.index_exp[1, -1, 1],
    np.index_exp[-1, -1, 1],
    np.index_exp[-2, -1, 1],
    np.index_exp[0, -2, 1],
    np.index_exp[1, -2, 1],
    np.index_exp[-1, -2, 1],
    np.index_exp[-2, -2, 1],
    np.index_exp[0, 0, -1],
    np.index_exp[1, 0, -1],
    np.index_exp[-1, 0, -1],
    np.index_exp[-2, 0, -1],
    np.index_exp[0, 1, -1],
    np.index_exp[1, 1, -1],
    np.index_exp[-1, 1, -1],
    np.index_exp[-2, 1, -1],
    np.index_exp[0, -1, -1],
    np.index_exp[1, -1, -1],
    np.index_exp[-1, -1, -1],
    np.index_exp[-2, -1, -1],
    np.index_exp[0, -2, -1],
    np.index_exp[1, -2, -1],
    np.index_exp[-1, -2, -1],
    np.index_exp[-2, -2, -1],
    np.index_exp[0, 0, -2],
    np.index_exp[1, 0, -2],
    np.index_exp[-1, 0, -2],
    np.index_exp[-2, 0, -2],
    np.index_exp[0, 1, -2],
    np.index_exp[1, 1, -2],
    np.index_exp[-1, 1, -2],
    np.index_exp[-2, 1, -2],
    np.index_exp[0, -1, -2],
    np.index_exp[1, -1, -2],
    np.index_exp[-1, -1, -2],
    np.index_exp[-2, -1, -2],
    np.index_exp[0, -2, -2],
    np.index_exp[1, -2, -2],
    np.index_exp[-1, -2, -2],
    np.index_exp[-2, -2, -2],
    np.index_exp[0:1, :],
    np.index_exp[1:, :],
    np.index_exp[-1:, :],
    np.index_exp[-2:, :],
    np.index_exp[0:1, ...],
    np.index_exp[1:, ...],
    np.index_exp[-1:, ...],
    np.index_exp[-2:, ...],
    np.index_exp[0:, 0:],
    np.index_exp[1:, 0:],
    np.index_exp[-1:, 0:],
    np.index_exp[-2:, 0:],
    np.index_exp[0:, 1:],
    np.index_exp[1:, 1:],
    np.index_exp[-1:, 1:],
    np.index_exp[-2:, 1:],
    np.index_exp[0:, -1:],
    np.index_exp[1:, -1:],
    np.index_exp[-1:, -1:],
    np.index_exp[-2:, -1:],
    np.index_exp[0:, -2:],
    np.index_exp[1:, -2:],
    np.index_exp[-1:, -2:],
    np.index_exp[-2:, -2:],
    np.index_exp[0:, 0:, :],
    np.index_exp[1:, 0:, :],
    np.index_exp[-1:, 0:, :],
    np.index_exp[-2:, 0:, :],
    np.index_exp[0:, 1:, :],
    np.index_exp[1:, 1:, :],
    np.index_exp[-1:, 1:, :],
    np.index_exp[-2:, 1:, :],
    np.index_exp[0:, -1:, :],
    np.index_exp[1:, -1:, :],
    np.index_exp[-1:, -1:, :],
    np.index_exp[-2:, -1:, :],
    np.index_exp[0:, -2:, :],
    np.index_exp[1:, -2:, :],
    np.index_exp[-1:, -2:, :],
    np.index_exp[-2:, -2:, :],
    np.index_exp[0:, 0:, ...],
    np.index_exp[1:, 0:, ...],
    np.index_exp[-1:, 0:, ...],
    np.index_exp[-2:, 0:, ...],
    np.index_exp[0:, 1:, ...],
    np.index_exp[1:, 1:, ...],
    np.index_exp[-1:, 1:, ...],
    np.index_exp[-2:, 1:, ...],
    np.index_exp[0:, -1:, ...],
    np.index_exp[1:, -1:, ...],
    np.index_exp[-1:, -1:, ...],
    np.index_exp[-2:, -1:, ...],
    np.index_exp[0:, -2:, ...],
    np.index_exp[1:, -2:, ...],
    np.index_exp[-1:, -2:, ...],
    np.index_exp[-2:, -2:, ...],
    np.index_exp[0:, 0:, 0:],
    np.index_exp[1:, 0:, 0:],
    np.index_exp[-1:, 0:, 0:],
    np.index_exp[-2:, 0:, 0:],
    np.index_exp[0:, 1:, 0:],
    np.index_exp[1:, 1:, 0:],
    np.index_exp[-1:, 1:, 0:],
    np.index_exp[-2:, 1:, 0:],
    np.index_exp[0:, -1:, 0:],
    np.index_exp[1:, -1:, 0:],
    np.index_exp[-1:, -1:, 0:],
    np.index_exp[-2:, -1:, 0:],
    np.index_exp[0:, -2:, 0:],
    np.index_exp[1:, -2:, 0:],
    np.index_exp[-1:, -2:, 0:],
    np.index_exp[-2:, -2:, 0:],
    np.index_exp[0:, 0:, 1:],
    np.index_exp[1:, 0:, 1:],
    np.index_exp[-1:, 0:, 1:],
    np.index_exp[-2:, 0:, 1:],
    np.index_exp[0:, 1:, 1:],
    np.index_exp[1:, 1:, 1:],
    np.index_exp[-1:, 1:, 1:],
    np.index_exp[-2:, 1:, 1:],
    np.index_exp[0:, -1:, 1:],
    np.index_exp[1:, -1:, 1:],
    np.index_exp[-1:, -1:, 1:],
    np.index_exp[-2:, -1:, 1:],
    np.index_exp[0:, -2:, 1:],
    np.index_exp[1:, -2:, 1:],
    np.index_exp[-1:, -2:, 1:],
    np.index_exp[-2:, -2:, 1:],
    np.index_exp[0:, 0:, -1:],
    np.index_exp[1:, 0:, -1:],
    np.index_exp[-1:, 0:, -1:],
    np.index_exp[-2:, 0:, -1:],
    np.index_exp[0:, 1:, -1:],
    np.index_exp[1:, 1:, -1:],
    np.index_exp[-1:, 1:, -1:],
    np.index_exp[-2:, 1:, -1:],
    np.index_exp[0:, -1:, -1:],
    np.index_exp[1:, -1:, -1:],
    np.index_exp[-1:, -1:, -1:],
    np.index_exp[-2:, -1:, -1:],
    np.index_exp[0:, -2:, -1:],
    np.index_exp[1:, -2:, -1:],
    np.index_exp[-1:, -2:, -1:],
    np.index_exp[-2:, -2:, -1:],
    np.index_exp[0:, 0:, -2:],
    np.index_exp[1:, 0:, -2:],
    np.index_exp[-1:, 0:, -2:],
    np.index_exp[-2:, 0:, -2:],
    np.index_exp[0:, 1:, -2:],
    np.index_exp[1:, 1:, -2:],
    np.index_exp[-1:, 1:, -2:],
    np.index_exp[-2:, 1:, -2:],
    np.index_exp[0:, -1:, -2:],
    np.index_exp[1:, -1:, -2:],
    np.index_exp[-1:, -1:, -2:],
    np.index_exp[-2:, -1:, -2:],
    np.index_exp[0:, -2:, -2:],
    np.index_exp[1:, -2:, -2:],
    np.index_exp[-1:, -2:, -2:],
    np.index_exp[-2:, -2:, -2:],
    np.index_exp[:, 1],
    np.index_exp[:, 1, :],
    np.index_exp[1, :],
    np.index_exp[1, :, :],
    np.index_exp[:, :, 1],
    np.index_exp[:, 1],
    np.index_exp[0:1, 1, 1:],
    np.index_exp[1, 0:1, 1:],
    np.index_exp[0:1, 1:, 1],
    np.index_exp[1, 0:1, 1],
    np.index_exp[:3, 1, 3:],
    np.index_exp[3, 3, 3:],
]

slices = [
    slice(None, None, None),
    slice(None, 1, None),
    slice(None, -1, None),
    slice(None, -2, None),
    slice(0, 1, None),
    slice(0, -1, None),
    slice(0, -2, None),
    slice(1, -1, None),
    slice(1, -2, None),
    slice(-2, -1, None),
    slice(0, 1, 1),
    slice(0, -1, 1),
    slice(0, -2, 1),
    slice(1, -1, 1),
    slice(1, -2, 1),
    slice(-2, -1, 1),
    slice(0, 1, 1),
    slice(0, -2, 1),
    slice(1, -1, 1),
    slice(1, -2, 1),
    slice(-2, -1, 1),
]

tuples = [
    (-2,),
    (-1,),
    (1,),
    (0,),
    (1, 0),
    (-1, -1),
    (-1, -2),
    (-2, -1),
    (-2, -2),
    (-1, 1),
    (-2, 1),
    (0, -2),
    (0, -1),
    (0, 1),
    (-1, 0),
    (-2, 0),
    (0, 0),
    (1, 1),
    (1, -1),
    (1, -2),
    (0, -1, -1),
    (0, -2, -1),
    (-1, 1, -1),
    (-2, 1, -1),
    (0, -1, 1),
    (0, -2, 1),
    (-1, 1, 1),
    (-2, 1, 1),
    (0, -1, -2),
    (0, -2, -2),
    (1, -1, 1),
    (1, -2, 1),
    (-2, -2, -2),
    (1, -1, -1),
    (1, -2, -1),
    (1, -1, -2),
    (1, -2, -2),
    (0, 0, -1),
    (0, 0, -2),
    (0, 0, 1),
    (1, 0, 1),
    (1, 0, -1),
    (-1, -1, -1),
    (-1, -1, 1),
    (-1, -2, 1),
    (-2, -1, 1),
    (-2, -2, 1),
    (1, 1, -2),
    (-1, -2, -1),
    (-2, -1, -1),
    (-2, -2, -1),
    (-1, -1, -2),
    (-1, -2, -2),
    (-2, -1, -2),
    (-1, 0, 1),
    (-2, 0, 1),
    (-1, 0, -1),
    (-2, 0, -1),
    (-1, 0, -2),
    (-1, 1, -2),
    (-2, 0, -2),
    (-2, 1, -2),
    (0, 1, -1),
    (0, 1, -2),
    (0, 1, 1),
    (1, 0, -2),
    (1, 1, -1),
    (1, 1, 1),
]

ints = [0, 1, -1, -2]
misc = [...]

# an ellipsis beside indices for every axis stands for no axis at all, as in numpy
ellipsis_for_no_axis = [
    np.index_exp[..., 0, :, :],
    np.index_exp[0, ..., :, :],
    np.index_exp[0, :, ..., :],
    np.index_exp[0, :, :, ...],
    np.index_exp[0, 0, :, ...],
    np.index_exp[0, 0, ..., :],
    np.index_exp[0, ..., 0, :],
    np.index_exp[..., 0, 0, :],
    np.index_exp[:, 0, 0, ...],
    np.index_exp[0, :, 0, ...],
    np.index_exp[0, :, ..., 0],
]

valid_index_exps = misc + np_index_exp_params + slices + tuples + ints + ellipsis_for_no_axis

invalid_index_exps = [
    np.index_exp[0:0, 1],
    np.index_exp[:, 1:1:1],
    np.index_exp[True],
    np.index_exp[False],
    np.index_exp[None],
    (None,),
    (
        None,
        None,
    ),
    (
        1,
        None,
    ),
    (
        None,
        1,
    ),
    (None, None, None),
    (None, 1, None),
    (1, None, None),
    (None, None, 1),
    (1, 1, None),
    (None, 1, 1),
    (1, None, 1),
    (True,),
    (
        True,
        True,
    ),
    (
        True,
        1,
    ),
    (
        1,
        True,
    ),
    (
        True,
        True,
        True,
    ),
    (False,),
    (
        False,
        False,
    ),
    (
        False,
        1,
    ),
    (
        1,
        False,
    ),
    (
        False,
        False,
        False,
    ),
    (
        True,
        False,
        False,
    ),
    (
        True,
        True,
        False,
    ),
    (
        True,
        False,
        True,
    ),
    (
        False,
        True,
        False,
    ),
    (
        False,
        True,
        True,
    ),
    (
        False,
        True,
    ),
    (
        True,
        False,
    ),
    (False, True, 1),
    (True, False, 1),
    (
        1,
        False,
        True,
    ),
    (
        1,
        True,
        False,
    ),
    (
        False,
        1,
        True,
    ),
    (
        True,
        1,
        False,
    ),
    (
        True,
        False,
        None,
    ),
    (
        False,
        True,
        None,
    ),
    (
        1,
        False,
        None,
    ),
    (
        1,
        True,
        None,
    ),
    (
        True,
        None,
        False,
    ),
    (
        False,
        None,
        True,
    ),
    (
        True,
        None,
        1,
    ),
    (
        False,
        None,
        1,
    ),
    (
        None,
        True,
        False,
    ),
    (
        None,
        False,
        True,
    ),
    (
        None,
        1,
        False,
    ),
    (
        None,
        1,
        True,
    ),
    np.index_exp[:, :, :, :],
    np.index_exp[..., ...],
    np.index_exp[0, :, :, :],
    np.index_exp[:, 0, :, :],
    np.index_exp[:, :, 0, :],
    np.index_exp[:, :, :, 0],
    np.index_exp[0, 0, :, :],
    np.index_exp[0, :, 0, :],
    np.index_exp[0, :, :, 0],
    np.index_exp[:, 0, 0, :],
    np.index_exp[:, 0, :, 0],
    np.index_exp[:, :, 0, 0],
    np.index_exp[0, 0, 0, :],
    np.index_exp[0, 0, :, 0],
    np.index_exp[0, :, 0, 0],
    np.index_exp[:, 0, 0, 0],
    np.index_exp[1, 0, 0, 1],
    np.index_exp[-1, -1, -1, -1],
    np.index_exp[0:1, :, :, :],
    np.index_exp[1:, :, :, :],
    np.index_exp[-1:, :, :, :],
    slice(None, 0, None),
    slice(0, 0, None),
    slice(1, 0, None),
    slice(1, 1, None),
    slice(-1, 0, None),
    slice(-1, 1, None),
    slice(-1, -1, None),
    slice(-1, -2, None),
    slice(-2, 0, None),
    slice(-2, 1, None),
    slice(-2, -2, None),
    slice(0, 0, 1),
    slice(1, 0, 1),
    slice(1, 1, 1),
    slice(-1, 0, 1),
    slice(-1, 1, 1),
    slice(-1, -1, 1),
    slice(-1, -2, 1),
    slice(-2, 0, 1),
    slice(-2, 1, 1),
    slice(-2, -2, 1),
    slice(0, 0, 1),
    slice(1, 0, 1),
    slice(1, 1, 1),
    slice(-1, 0, 1),
    slice(-1, 1, 1),
    slice(-1, -1, 1),
    slice(-1, -2, 1),
    slice(-2, 0, 1),
    slice(-2, 1, 1),
    slice(-2, -2, 1),
]


# ---------------------------------------------------------------------------------------------------------------------
# Steps (D5/D6): index expressions with a step other than 1, for arrays and varrays of one to four dimensions
# ---------------------------------------------------------------------------------------------------------------------

# Shapes of the arrays of the step tests, by the number of dimensions
STEP_SHAPES: dict[int, tuple[int, ...]] = {1: (24,), 2: (12, 10), 3: (12, 10, 8), 4: (6, 5, 4, 6)}

# Grids of the varrays of the step tests: one Array, uneven Arrays along every axis (sizes that do not divide evenly
# by the steps), and an Array of one cell on the first axis (smaller than any step)
STEP_VGRIDS: dict[int, list[tuple[int, ...]]] = {
    1: [(1,), (3,), (24,)],
    2: [(1, 1), (3, 5), (12, 1)],
    3: [(1, 1, 1), (3, 5, 2), (12, 1, 1)],
    4: [(1, 1, 1, 1), (3, 5, 2, 2), (6, 1, 4, 1)],
}

# Integer steps that select at least one cell on every axis of the shape of their dimensionality;
# the result equals numpy's
STEPPED_INDEX_EXPS: dict[int, list] = {
    1: [
        np.index_exp[::2],
        np.index_exp[::3],
        np.index_exp[1::3],
        np.index_exp[::5],
        np.index_exp[::7],
        np.index_exp[::23],
        np.index_exp[::24],
        np.index_exp[2:23:4],
        np.index_exp[::-1],
        np.index_exp[::-2],
        np.index_exp[23:2:-3],
        np.index_exp[-1::-4],
        np.index_exp[-2:-20:-2],
        np.index_exp[9:1:-1],
        np.index_exp[5:6:2],
        np.index_exp[1:0:-1],
        np.index_exp[-5::2],
        np.index_exp[..., ::2],
    ],
    2: [
        np.index_exp[::2],
        np.index_exp[::-1],
        np.index_exp[::2, ::3],
        np.index_exp[::-2, ::-3],
        np.index_exp[:, ::-2],
        np.index_exp[3, ::2],
        np.index_exp[::2, 5],
        np.index_exp[1:12:2, 9:0:-2],
        np.index_exp[..., ::2],
        np.index_exp[::-1, ::-1],
        np.index_exp[::7, ::11],
        np.index_exp[11:2:-3, 1::3],
        np.index_exp[-5::2, :-1:2],
        np.index_exp[5:6:2, 1:0:-1],
    ],
    3: [
        np.index_exp[::2],
        np.index_exp[::3],
        np.index_exp[1::3],
        np.index_exp[::5],
        np.index_exp[::7],
        np.index_exp[::11],
        np.index_exp[::12],
        np.index_exp[2:11:4],
        np.index_exp[::-1],
        np.index_exp[::-2],
        np.index_exp[11:2:-3],
        np.index_exp[-1::-4],
        np.index_exp[-2:-9:-2],
        np.index_exp[9:1:-1],
        np.index_exp[::2, ::3],
        np.index_exp[::-2, ::-3],
        np.index_exp[:, ::-2, 1::3],
        np.index_exp[3, ::2],
        np.index_exp[::2, 5, ::-1],
        np.index_exp[1:12:2, 9:0:-2, ::4],
        np.index_exp[..., ::2],
        np.index_exp[::2, ..., ::-3],
        np.index_exp[::-1, ::-1, ::-1],
        np.index_exp[5:6:2],
        np.index_exp[1:0:-1],
        np.index_exp[-5::2, :-1:2, 2::2],
    ],
    4: [
        np.index_exp[::2],
        np.index_exp[::-1],
        np.index_exp[::2, ::3, ::-1, ::2],
        np.index_exp[1::3, ::-2, 1::2, ::5],
        np.index_exp[::7, ::11, ::5, ::7],
        np.index_exp[3, ::2, ::-1, 1],
        np.index_exp[::-1, 2, ::3, ::-2],
        np.index_exp[..., ::-2],
        np.index_exp[::2, ..., ::3],
        np.index_exp[-1::-2, ::-1, 3:0:-1, ::-3],
        np.index_exp[1:6:2, 4:0:-2, ::2, 5:1:-2],
        np.index_exp[5:6:2, 1:0:-1, 3:4:3, 2:1:-1],
    ],
}

# The same step on the first axis only, on every axis at once, and the three steps mixed across the axes
UNIFORM_STEPS = (3, -2, -3)


def _uniform_step_exps(ndim: int) -> list:
    """Index expressions with the steps of ``UNIFORM_STEPS`` on the first axis, on every axis and mixed across the axes.

    :param ndim: number of dimensions
    :returns: index expressions without duplicates (slices are not hashable before Python 3.12, hence the list)
    """
    exps: list = []
    for step in UNIFORM_STEPS:
        for exp in (np.index_exp[::step], (slice(None, None, step),) * ndim):
            if exp not in exps:
                exps.append(exp)
    mixed = tuple(slice(None, None, UNIFORM_STEPS[axis % len(UNIFORM_STEPS)]) for axis in range(ndim))
    if mixed not in exps:
        exps.append(mixed)
    return exps


for _ndim, _exps in STEPPED_INDEX_EXPS.items():
    _exps.extend(exp for exp in _uniform_step_exps(_ndim) if exp not in _exps)


def _duration_id(duration: timedelta) -> str:
    """Readable id of a timedelta: whole days, else whole hours, else seconds.

    :param duration: the timedelta
    :returns: id
    """
    seconds = duration.total_seconds()
    if seconds % 86400 == 0:
        return f"{int(seconds // 86400)}d"
    if seconds % 3600 == 0:
        return f"{int(seconds // 3600)}h"
    return f"{seconds:g}s"


def _item_id(item: object) -> str:
    """Readable id of one index of an expression.

    :param item: the index: a slice, an ellipsis, a number, a label, a datetime or a timedelta
    :returns: id
    """
    if isinstance(item, slice):
        bounds = [item.start, item.stop] if item.step is None else [item.start, item.stop, item.step]
        return ":".join("" if v is None else _item_id(v) for v in bounds)
    if item is Ellipsis:
        return "..."
    if isinstance(item, datetime):
        return item.isoformat()
    if isinstance(item, timedelta):
        return _duration_id(item)
    return repr(item) if isinstance(item, str) else str(item)


def exp_id(index_exp: object) -> str:
    """Readable id of an index expression, as it is written between the brackets.

    :param index_exp: index expression: a tuple of indices or a single index
    :returns: id
    """
    items = index_exp if isinstance(index_exp, tuple) else (index_exp,)
    return "[" + ",".join(_item_id(item) for item in items) + "]"


def as_params(index_exps: list) -> list:
    """Wrap index expressions into pytest params with readable ids.

    :param index_exps: index expressions
    :returns: pytest params
    """
    return [pytest.param(index_exp, id=exp_id(index_exp)) for index_exp in index_exps]


valid_index_exp_params = as_params(valid_index_exps)
invalid_index_params = as_params(invalid_index_exps)


def array_step_cases() -> list:
    """Every step expression on the array shape of its dimensionality.

    :returns: pytest params ``(shape, index_exp)``
    """
    return [
        pytest.param(shape, index_exp, id=f"{'x'.join(map(str, shape))}{exp_id(index_exp)}")
        for ndim, shape in STEP_SHAPES.items()
        for index_exp in STEPPED_INDEX_EXPS[ndim]
    ]


def varray_step_cases(every_grid: bool = True) -> list:
    """Every step expression on the grids of the varray shape of its dimensionality.

    :param every_grid: ``True`` for every grid, ``False`` for the first grid of every dimensionality only
        (for the checks that do not depend on the grid)
    :returns: pytest params ``((shape, vgrid), index_exp)``
    """
    return [
        pytest.param(
            (shape, vgrid),
            index_exp,
            id=f"{'x'.join(map(str, shape))}-grid{'x'.join(map(str, vgrid))}{exp_id(index_exp)}",
        )
        for ndim, shape in STEP_SHAPES.items()
        for vgrid in (STEP_VGRIDS[ndim] if every_grid else STEP_VGRIDS[ndim][:1])
        for index_exp in STEPPED_INDEX_EXPS[ndim]
    ]


# Steps that are not valid on any array, whatever the dimension types
invalid_step_index_exps = [
    np.index_exp[::0],
    np.index_exp[:, ::0],
    np.index_exp[::True],
    np.index_exp[::False],
    np.index_exp[:, :, ::True],
    np.index_exp[0:10:0],
]

invalid_step_index_exp_params = as_params(invalid_step_index_exps)
