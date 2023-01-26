#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 07:18:54 2023

@author: anvaari
"""

class DatabaseExceptions(Exception):
    """Exception class from which every exception in this library will derive.
         It enables other projects using this library to catch all errors coming
         from the library with a single "except" statement
    """
    pass

class InsertError(DatabaseExceptions):
    """
    When error accured during insert this exception arise.
    """
    pass

class ExecutionError(DatabaseExceptions):
    """
    When error accured during execution of query, this exception arise.
    """


class DataExceptions(Exception):
    """
    """
    pass