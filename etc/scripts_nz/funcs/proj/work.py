from os.path import join
from pickle import load as pickle_load

from funcs import RAW_DATA
from pandas import read_csv


def project_work_data(workdir):

    raw_pop_data = pickle_load(open(join(workdir, "workdir.pickle"), "rb"))
    proj_data = read_csv(join(RAW_DATA["projection"]["business"]["labours"]))
