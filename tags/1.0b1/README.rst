====
PaPy
====

Description
-----------

PaPy, which stands for parallel pipelines in Python, is a highly flexible
framework that addresses the problem of creating scalable workflows to process
or generate data. A workflow is created from Python functions(nodes) with
well-defined  call/return semantics, connected by pipes(edges) into a 
directed acyclic graph.  Given the topology and input data, these functions are 
composed into nested higher-order maps, which are transparently and robustly
evaluated in parallel on a single computer or remote hosts. The local and remote
computational resources can be flexibly pooled and assigned to functional nodes.
This allows to easily load-balance a pipeline and optimize the throughput. Data 
traverses the graph in batches of adjustable size: a trade-off between
lazy-evaluation, parallelism and memory consuption. The simplicity and 
flexibility of distributed workflows using PaPy bridges the gap between desktop
and grid.

Installation
------------

The easiest way to get PaPy is if you have setuptools_ installed::

        easy_install papy

.. _setuptools: http://peak.telecommunity.com/DevCenter/EasyInstall