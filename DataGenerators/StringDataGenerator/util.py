#!/usr/bin/env python3
# coding: utf-8


import os
import shutil
import math

########################Start of helper functions########################
LOG_FILE = "data_generator.log"

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    OKMAGENTA = '\033[35m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def printWithColor(message, color):
    print("{}{}{}".format(color, message, bcolors.ENDC))


def printSuccess(message):
    printWithColor(message, bcolors.OKGREEN)


def printFail(message):
    printWithColor(message, bcolors.FAIL)


def printHighlight(message):
    printWithColor(message, bcolors.OKCYAN)


def print2File(message, fileName, file=__file__):
    with open(fileName, 'a+') as f:
        f.write("{}".format(message))


def print2Log(message, file=__file__):
    with open(LOG_FILE, 'a+') as f:
        f.write("\n----------------------{}----------------------\n".format(file))
        f.write("{}".format(message))
        f.write("\n----------------------{}----------------------\n".format(file))


def fileFolderExists(file):
    return os.path.exists(file)


def createFolder(folder):
    if not fileFolderExists(folder):
        os.mkdir(folder)
        printHighlight("{} was created!".format(folder))

def removeFolder(folder):
    if fileFolderExists(folder):
        printHighlight(f"Removing {folder}")
        shutil.rmtree(folder)

########################End of helper functions##########################


def calcNumberOfRecords(outputFileSize, schemaSize):
    if outputFileSize % schemaSize != 0:
        printFail(f"outputFileSize {outputFileSize} is not a multiplicative of {schemaSize}. Therefore, we will create one additional tuple!")

    return math.ceil(float(outputFileSize) / schemaSize)
