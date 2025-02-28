########################################################
Butler management of quantum graph storage and execution
########################################################

.. abstract::

   This technote proposes new data structures and interfaces for storing and accessing graphs of predicted processing :class:(``QuantumGraph`` objects), with the goal of making them usable for reporting on provenance after processing has completed.

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

.. default-domain:: py

.. py:currentmodule:: lsst.pipe.base

Proposal Overview
=================

We have long recognized that the simplest possible path to a graph-based butler provenance system is something along the lines of "store a :class:`QuantumGraph` in the butler".
The current :class:`QuantumGraph` class and on-disk format aren't quite up to this (though the latter is close), as we'll detail in :ref:`current-status`, and so this will involve replacements for both, described in :ref:`file-format` and :ref:`new-python-interfaces`.
After execution, the graph will be stripped of information that is duplicated in the data repository (dimension and datastore records) and augmented with status information, and then ingested as not one but *multiple* butler datasets, leveraging existing butler support for multiple datasets sharing the same artifact (via a sophisticated `~lsst.daf.butler.Formatter`).
This is described in :ref:`butler-integration`.

.. _current-status:

Current Status
==============

QuantumGraph
------------

The :class:class::`QuantumGraph` class is our main representation of processing predictions.
In addition to information about task executions ("quanta") and the datasets they consume and produce, it stores dimension records and sometimes datastore records, allowing tasks to be executed with a :class:`~lsst.daf.butler.QuantumBackedButler` that avoids database operations until the complete graph is completed.
The :class:class::`QuantumGraph` on-disk format has been carefully designed to be space-efficient while still allowing for fast reads of individual quanta (even over object stores and http), which is important for how the graph is used to execute tasks at scale.

In terms of supporting execution, the current :class:`QuantumGraph` is broadly fine, but we would like to start using the same (or closely-related) interfaces and file format for *provenance*, i.e. reporting on the relationships between datasets and quanta after execution has completed.
On this front the current :class:`QuantumGraph` has some critical limitations:

- It requires all task classes to be imported and all task configs to be evaluated (which is a code execution, due to the way `lsst.pex.config` works) when it is read from disk.
  This makes it very hard to keep a persisted :class:`QuantumGraph` readable as the software stack changes, and in some contexts it could be a security problem.
  The :class:`~pipeline_graph.PipelineGraph` class (which did not exist when :class:`QuantumGraph` was introduced) has a serialization system that avoids this problem that :class:`QuantumGraph` could delegate to, if we were to more closely integrate them.

- :class:`QuantumGraph` is built on a directed acyclic graph of quantum-quantum edges; datasets information is stored separately.
  Provenance queries often treat the datasets as primary and the quanta as secondary, and for this a "bipartite" graph structure (with quantum-dataset and dataset-quantum edges) would be more natural and allow us to better leverage third-party libraries like `networkx <https://networkx.org/>`__.

- For provenance queries, we often want a shallow but broad load of the graph, in which we read the relationships between many quanta and datasets in order to traverse the graph, but do not read the details of the vast majority of either.
  The current on-disk format is actually already well-suited for this, but the in-memory data structure and interface are not.

- Provenance queries can sometimes span multiple :attr:`~lsst.daf.butler.CollectionType.RUN` collections, and hence those queries may span multiple files on disk.
  :class:`QuantumGraph` currently expects a one-to-one mapping between instances and files.
  While we could add a layer on top of :class:`QuantumGraph` to facilitate queries over multiple files, it may makes more sense to have a new in-memory interface that operates directly on the stored outputs of multiple runs.
  This gets particularly complicated when those runs have overlapping data IDs and tasks, e.g. a "rescue" run that was intended to fix problems in a previous one.

- While the same graph structure and much of the same metadata (data IDs in particular) is relevant for both execution and provenance queries, there is some information only needed in the graph for execution (dimension and datastore records, which are only duplicated into the graph to avoid database hits during execution) as well as status information that can only be present in post-execution provenance graphs.

Finally, while not a direct problem for provenance, the :class:`QuantumGraph` serialization system is currently complicated by a lot of data ID / dimension record [de]normalization logic.
This was extremely important for storage and memory efficiency, but it's something we think we can avoid in this rework almost entirely, largely by making :class:`lsst.daf.butler.Quantum` instances only when needed, rather than using them as a key part of the internal representation.

QuantumProvenanceGraph
----------------------

The ``QuantumProvenanceGraph`` class (what's used to back the ``pipetask report`` tool) is constructed from a sequence of already-executed :class:`~lsst.pipe.base.QuantumGraph` instances and a :class:`~lsst.daf.butler.Butler`.
It traverses the graphs, queries the butler for dataset existence and task metadata, and assembles this into a summary of task and dataset provenance that is persisted to JSON (as well as summary-of-the-summary tables of counts).

``QuantumProvenanceGraph`` is not itself persistable (the summary that can be saved drops all relationship information), and is focused mostly on tracking the evolution of particular problematic quanta and dataset across multiple runs.
While it is already very useful, for the purpose of this technote it is best considered a useful testbed and prototyping effort for figuring out what kinds of provenance information one important class of users (campaign pilots, and developers acting as mini-campaign pilots) needs, and especially how best to classify the many combinations of statuses that can coexist in a multi-run processing campaign.
Eventually we hope to incorporate all of the lessons learned into a new more efficient provenance system in which the backing data is fully managed by the :class:`~lsst.daf.butler.Butler`.

.. _onfile-format:

File Format
===========

TODO

.. _butler-integration:

Butler Integration
==================

In our current proposal, before and during task execution, a quantum graph file would not be managed by the butler, and it would continue be accessed via a regular Python class (see :ref:`new-python-interfaces`) that reads the file directly.
A future rework of ``pipetask`` (to make it more like ``bps`` in various respects) might change this, but this is considered out of scope for this technote.

After execution, the graph file would be rewritten from "execution mode" to "provenance mode", by:

- dropping all dimension and datastore records, since those are redundant with information already in the data repository;

- adding status information to the "thin quantum" (simple flags) and "extended quantum" sections (exception information when present, maybe logs and task metadata as well).
  The sources of this status information are described later in :ref:`Gathering Status Information`.

This provenance graph file would be ingested into a data repository as *multiple* datasets, using a special :class:`~lsst.daf.butler.Formatter` and existing :class:`~lsst.daf.butler.Butler` "shared datastore artifact" functionality (TBD whether this is more like ``zip`` support or the DECam ``raw`` formatter).
The primary dataset would represent the full graph, and would use a storage class with a suite of components and parameters that a higher-level provenance-query interface (in ``pipe_base``) could use to perform efficient partial reads through the butler.
In addition to that primary graph dataset, we would ingest one dataset for each quantum (again, backed by the same file and partial reads), with one dataset type for each task, and dimensions matching those of the task.
This could replace the ``_log`` and ``_metadata`` dataset types (these would become components), meaning an overall reduction in the number of dataset rows relative to day, and would also contain additional, more structured status information (especially in the case of failures).
In addition, these would be ingested with dataset UUIDs that are the same as the quantum UUIDs, which lets us use the regular butler system to identify quanta that can then be used in provenance searches.

Batch Execution
---------------

In batch execution, we already have a final job that traverses the graph and gathers status information (when it checks for dataset existence), before ingesting regular processing outputs into the data repository.
It would be extremely natural for this to be the same step that transforms the graph from execution form to provenance form and then ingests the provenance datasets.
Because batch workflows are one-to-one with :attr:`~lsst.daf.butler.CollectionType.RUN` collections, we would ingest one graph dataset for the full workflow (with empty dimensions) and one status dataset for each quantum (with the dimensions of the task), all backed by a single file.

Prompt Processing
-----------------

In prompt processing, tasks are executed in a transient pod with a local data repository that is entirely distinct from the persistent central butler that outputs are ultimately transferred back to.
In this case, it makes sense to transform the graph from execution form to provenance form as part of that transfer.
It might be possible for this to eventually be the same code that is used in batch execution, but to avoid disruption to prompt processing's current transfer systems we may want to provide a more self-contained command to transform and ingest a graph.
Because prompt processing workflows are per-\ ``{visit, detector}``, the full-graph dataset that corresponds directly to the ingested file artifact would have those dimensions.
The per-quantum status datasets would of course still have the dimensions of the task.

.. _gathering-status-information:

Gathering Status Information
============================

TODO

.. _new-python-interfaces:

New Python Interfaces
=====================

TODO

References
==========


.. bibliography::
