######################################################################
Reimagining quantum graphs for post-execution transfers and provenance
######################################################################

.. abstract::

   This technote proposes new approaches for ingesting pipeline execution outputs into butler repositories, with two distinct goals: storing input-output provenance in butler repositories (in both prompt processing and batch), and speeding up (and probably spreading out) the work of ingesting batch-processing outputs into the butler database.
   centered on new data structures for storing pre- and post-execution quantum graphs.
   These new approaches center around new in-memory and on-disk data structures for different quantum graphs in different stages of execution.

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

.. default-domain:: py

.. py:currentmodule:: lsst.pipe.base

Proposal Overview
=================

We have long recognized that the simplest possible path to a graph-based butler provenance system is something along the lines of "store a :class:`QuantumGraph` in the butler".
The current :class:`QuantumGraph` class and on-disk format aren't quite up to this (though the latter is close), as we'll detail in :ref:`current-status`, and so this will involve replacements for both, described in :ref:`file-formats` and :ref:`variants`.
After execution, the graph will be stripped of information that is duplicated in the data repository (dimension and datastore records) and augmented with status information, and then ingested as not one but *multiple* butler datasets, leveraging existing butler support for multiple datasets sharing the same artifact (via a sophisticated :class:`~lsst.daf.butler.Formatter`).
This is described in :ref:`provenance-graphs`.

A key consideration here is that a pre-execution graph does not become a post-execution graph instantaneously; even if we ignore the actual time it takes to run the pipeline and consider a monolithic transfer as in today's BPS ``finalJob``, the process can take many hours and can easily be interrupted.
And while the many detector-level prompt processing jobs are wholly independent, they are so numerous that for provenance storage we want to consider them pieces of much a single nightly (or multi-night) job that is updated incrementally.
To address this, some of the new quantum graph data structures here are designed to be updated in-place on disk, via append-only writes\ [#append-as-chunks]_, to reflect completed or failed quanta and existence-checked dataset artifacts.

In batch, we envision something like the following process for generating and running a workflow:

- A :ref:`predicted quantum graph <predicted-graphs>` is generated and saved to a file, just as it is today (albeit with a new file format).
- BPS reads the predicted quantum graph file and generates its own workflow graph, adding nodes for ``pipetaskInit`` and possibly one or more transfer jobs.
- In the ``pipetaskInit`` job, the output collections are created and init-output datasets are ingested into the butler database (not just written to storage).
  In addition, a new, :ref:`aggregation quantum graph <aggregation-graphs>` is created from the "predicted" graph and stored in the BPS submit directory (or some other BPS-configurable writeable location).
- At various points while the graph is being executed (see :ref:`invoking-aggregation`), a new tool is invoked to aggregate provenance and ingest datasets into the butler repository, reading and updating the on-disk aggregation graph as it goes.
  The aggregation graph data structure will allow the tool to quickly determine which quanta have already been aggregated, which may be ready to be aggregated (because they have run, are currently running, or could soon be running), and which are definitely not ready (because upstream quanta failed or have not yet run), and focus its attention accordingly.
- When we want to consider the run complete, either because it finished or we want to fully cancel it, we run the same tool with an additional flag that transforms the append-only aggregation graph into a read-only :ref:`provenance quantum graph <provenance-graphs>` when aggregation is complete.
  The provenance graph is then ingested into the butler.

In prompt processing, we expect small, per-detector predicted quantum graphs to be generated and executed in independent worker pods, just as they are today, and information about the outputs sent via Kafka system to a dedicated service for provenance gathering and ingestion, as is already planned for the near future.
As part of this proposal, worker pods would also need to send a version of the predicted quantum graph to the service, either via Kafka (if it's small enough) or an S3 put.
The ingestion service would then use that to append provenance to its own aggregation quantum graph file, rolling over to a new one and ingesting the old one only when the :attr:`~lsst.daf.butler.CollectionType.RUN` collection changes.

Eventually, we will want direct ``pipetask`` executions to operate the same way as BPS (in this and many other ways), but with all steps all handled by a single Python command.
This may be rolled out well after the batch and prompt-processing variants are, especially if provenance-hostile developer use cases for clobbering and restarting with different versions and configuration prove difficult to reconcile with having a single post-execution graph dataset for each :attr:`~lsst.daf.butler.CollectionType.RUN` collection.

.. [#append-as-chunks] In storage systems that do not support appending to files, we can simulate appends by writing numbered chunks to separate files, but for simplicity we will just refer to this as "appending" for the rest of this note.

.. _current-status:

Current Status
==============

QuantumGraph
------------

The :class:`QuantumGraph` class is our main representation of processing predictions.
In addition to information about predicted task executions ("quanta") and the datasets they consume and produce, it stores dimension records and sometimes datastore records, allowing tasks to be executed with a :class:`~lsst.daf.butler.QuantumBackedButler` that avoids database operations during execution.
The :class:`QuantumGraph` on-disk format has been carefully designed to be space-efficient while still allowing for fast reads of individual quanta (even over object stores and http), which is important for how the graph is used to execute tasks at scale.

In terms of supporting execution, the current :class:`QuantumGraph` is broadly fine, but we would like to start using the same (or closely-related) interfaces and file format for *provenance*, i.e. reporting on the relationships between datasets and quanta after execution has completed.
On this front the current :class:`QuantumGraph` has some critical limitations:

- It requires all task classes to be imported and all task configs to be evaluated (which is a code execution, due to the way :class:`~lsst.pex.config.Config` works) when it is read from disk.
  This makes it very hard to keep a persisted :class:`QuantumGraph` readable as the software stack changes, and in some contexts it could be a security problem.
  The :class:`~pipeline_graph.PipelineGraph` class (which did not exist when :class:`QuantumGraph` was introduced) has a serialization system that avoids this problem that :class:`QuantumGraph` could delegate to, if we were to more closely integrate them.

- :class:`QuantumGraph` is built on a directed acyclic graph of quantum-quantum edges; dataset information is stored separately.
  Provenance queries often treat the datasets as primary and the quanta as secondary, and for this a "bipartite" graph structure (with quantum-dataset and dataset-quantum edges) would be more natural and allow us to better leverage third-party libraries like `networkx <https://networkx.org/>`__.

- For provenance queries, we often want a shallow but broad load of the graph, in which we read the relationships between many quanta and datasets in order to traverse the graph, but do not read the details of the vast majority of either.
  The current on-disk format is actually already well-suited for this, but the in-memory data structure and interface are not.

- Provenance queries can sometimes span multiple :attr:`~lsst.daf.butler.CollectionType.RUN` collections, and hence those queries may span multiple files on disk.
  :class:`QuantumGraph` currently expects a one-to-one mapping between instances and files.
  While we could add a layer on top of :class:`QuantumGraph` to facilitate queries over multiple files, it may makes more sense to have a new in-memory interface that operates directly on the stored outputs of multiple runs.
  This gets particularly complicated when those runs have overlapping data IDs and tasks, e.g. a "rescue" run that was intended to fix problems in a previous one.

- While the same graph structure and much of the same metadata (data IDs in particular) are relevant for both execution and provenance queries, there is some information only needed in the graph for execution (dimension and datastore records) as well as status information that can only be present in post-execution provenance graphs.

Finally, while not a direct problem for provenance, the :class:`QuantumGraph` serialization system is currently complicated and bogged down by a lot of data ID / dimension record [de]normalization logic.
This was extremely important for storage and memory efficiency in the context in which the system originally evolved, but it's something we think we can avoid in this rework almost entirely, largely by making :class:`lsst.daf.butler.Quantum` instances only when needed, rather than using them as a key part of the internal representation.

QuantumProvenanceGraph
----------------------

The ``QuantumProvenanceGraph`` class (what's used to back the ``pipetask report`` tool) is constructed from a sequence of already-executed :class:`QuantumGraph` instances and a :class:`~lsst.daf.butler.Butler`.
It traverses the graphs, queries the butler for dataset existence and task metadata, and assembles this into a summary of task and dataset provenance that is persisted to JSON (as well as summary-of-the-summary tables of counts).

``QuantumProvenanceGraph`` is not itself persistable (the summary that can be saved drops all relationship information), and is focused mostly on tracking the evolution of particular problematic quanta and dataset across multiple runs.
While it is already very useful, for the purpose of this technote it is best considered a useful testbed and prototyping effort for figuring out what kinds of provenance information one important class of users (campaign pilots, and developers acting as mini-campaign pilots) needs, and especially how best to classify the many combinations of statuses that can coexist in a multi-run processing campaign.
Eventually we hope to incorporate all of the lessons learned into a new more efficient provenance system in which the backing data is fully managed by the :class:`~lsst.daf.butler.Butler`.

.. _file-formats:

New File Formats
================

:class:`QuantumGraph` files currently have two sections: a header that is always read in full, and a sequence of per-quantum blocks that are loaded only when that quantum is requested by UUID.
Aside from a tiny binary prelude to the header, they are stored as blocks of LZMA-compressed JSON, with byte-offsets to per-quantum blocks stored in the header.
Since the UUIDs we use to identify quanta and datasets in the public interface are large and random, they compress extremely poorly, and the current quantum graph format maps these to sequential integer IDs (only unique within a particular graph file) in order to save them only once.

In the new quantum graph file formats proposed here, we will similarly rely on compressed JSON indexed by byte offsets, with some important changes.
Instead of using a custom binary header and high-level file layout, we will use ``zip`` files to more easily aggregate multiple components (i.e. as "files" in a ``zip`` archive) that may be common to some graph variants but absent from others.
Each JSON component will be represented by a Pydantic model, and we will use ZStandard\ [#zstd-in-zip]_ for compression.
Pydantic representations of butler objects (e.g. :class:`~lsst.daf.butler.DatasetRef`) will be deliberately independent of the ones in ``daf_butler`` to avoid coupling the ``RemoteButler`` API model versions to the quantum graph serialization model version, except for types with schemas dependent on butler configuration (dimension and datastore records).

Unfortunately, while the ``zip`` format is great for aggregating a small number of files (or even thousands), the overheads involved in storing millions of files can be significant compared to the simpler, more lightweight byte-offset indexing used in the current :class:`QuantumGraph` files, in terms of both file size and partial-read times.
As a result we plan to continue to store some per-quantum and per-dataset information in "multi-block" files containing directly-concatenated blocks of compressed JSON.
To improve our ability to handle hard failures and file corruption, each block in a multi-block is preceded by its own (post-compression) size.

Each multi-block "file" is associated with a separate address "file" (i.e. in the same ``zip`` archive), which is a simple binary table in which each row holds a UUID, an internal integer ID, one or more byte offsets, and one or more byte counts (a single address file can index multiple multi-block files).
Address files can be sorted by UUID, which yields a typical-case O(1) lookup performance (since UUIDs are more or less draw from a uniform random distribution), with even the worst-case only O(log N).
As in the current quantum graph format, this will allow us to use these IDs instead of UUIDs in certain other components, making them compress much better.

A typical quantum graph file will thus be a single ``zip`` file (or in the :ref:`aggregation quantum graph <aggregation-graphs>` case, a directory) containing several simple "single-block" compressed-JSON header files, one or two address files (for quanta and possibly datasets) and one or more multi-block files.
While we will not attempt to hide the fact that these are ``zip`` files and very much intend to leverage that fact for debugging, we will continue to use a custom filename extension and do not plan to support third-party readers.

.. [#zstd-in-zip] https://facebook.github.io/zstd/.
   In our benchmarks on real QG data, ZStandard yielded compression ratios and decompression times as good or better than LZMA, with orders-of-magnitude faster compression times.  While ``zip`` does not support ZStandard directly as one of its compression algorithms, it works just fine to explicitly compress and decompress when write and reading files in a ``zip`` archive.

.. _variants:

New Quantum Graph Variants
==========================

.. _predicted-graphs:

Predicted Quantum Graphs
------------------------

A "predicted" quantum graph serves the same roles as the current one: reporting what tasks will be run and what datasets they produce and consume in order to plan execution, and supporting that execution by providing read-only metadata storage for a :class:`lsst.daf.butler.QuantumBackedButler`.

This graph is optimized for fast-and-deep reads of a small number of pre-identified quanta, fast-but-shallow reads of the full graph for BPS workflow graph generation, and fast conversion to later graph variants.
These different use cases will be represented by different public Python types (implemented on top of the same underlying Pydantic models) to reflect the fact that they load different subsets of the full on-disk content and have different capabilities.
The type constructed from a shallow read of the full graph models a directed acyclic graph (DAG) in which each node is a quantum, and datasets are elided.

During our transition period away from the old :class:`QuantumGraph` class, we will support complete bidirectional conversion between the old class and the new on-disk data structure.

The on-disk components of a predicted quantum graph are described in :ref:`predicted-components`.

.. _predicted-components:

.. list-table:: Predicated Quantum Graph Components
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - ``header``
     - single-block
     - Format version, collection metadata, quantum and dataset counts.
   * - ``pipeline_graph``
     - single-block
     - | Serialized :class:`~pipeline_graph.PipelineGraph`, including
       | all configuration.
   * - ``quantum_edges``
     - single-block
     - Edge list for a quantum-quantum DAG (using internal integer IDs).
   * - ``dimension_data``
     - single-block
     - | All butler dimension records referenced by any data ID in the graph.
       | While this nominally grows with the size of the full graph, when all
       | dimensions are stored (and compressed) together, the total size is
       | in practice always small compared to other components.
   * - ``thin_quanta``
     - single-block
     - Task labels and data IDs (only) for all quanta.
   * - ``init_quanta``
     - single-block
     - | Init-input and init-output dataset information, represented as one
       | special "init quantum" for each task, using the same model as
       | ``full_quanta``.
   * - ``full_quanta``
     - multi-block
     - | Input and output dataset information, including data IDs, UUIDs,
       | and datastore records for each quantum.
   * - ``quantum_addresses``
     - address (sorted by UUID)
     - | UUID to integer ID mapping for all quanta, byte offsets and sizes
       | for ``full_quanta``.

A deep read of just a few quanta (identified by UUID) would thus involve the following steps:

1. Read the ``header`` to verify file format versions and load general metadata.
2. Read the complete ``pipeline_graph``, ``dimension_data`` and ``init_quanta`` components.
3. Search the ``quantum_addresses`` file for the byte offsets and integer IDs for the UUIDs of interest.
4. Read the blocks of the ``full_quanta`` component that according to those byte offsets.
5. Use a :class:`~lsst.daf.butler.DimensionDataAttacher` to attach dimension records to all loaded quantum and dataset data IDs.

A shallow read of all quanta (e.g. for ``bps transform``) would read different components:

1. Read the ``header`` to verify file format versions and load general metadata.
2. Read the complete ``pipeline_graph``, ``quantum_edges``, ``thin_quanta``, ``init_quanta``, and ``quantum_addresses`` component.
3. Transform the internal IDs to UUIDs, and use the edge list to make a :class:`networkx.DiGraph` of all quanta.

.. _wire-graphs:

Wire Quantum Graphs
-------------------

A "wire" quantum graph is a lightweight version of the predicted graph that can be sent from prompt-processing worker pods to its ingestion service efficiently.
It does not need to support :class:`~lsst.daf.butler.QuantumBackedButler` and can assume its reader has access to a full butler.

The exact components in a wire quantum graph are still TBD, as they depend on how much information the Prompt Processing ingest service gets from other sources (e.g. does it already know the exact :class:`~pipeline_graph.PipelineGraph` the worker pods are running).
Because prompt-processing graphs only have a single quantum for each task (and rarely more than one dataset of each type), the expensive components are quite different from the expensive components in a large batch graph, and there is no need to support single-quantum partial reads with multi-block components and address files.
It is likely the most efficient data structure for a wire graph will just be a single JSON blob, mapped to a single parent Pydantic model that aggregates some of the models used in the predicted graph's components.

.. _aggregation-graphs:

Aggregation Quantum Graphs
--------------------------

An "aggregation" graph is used to represent the provenance of a currently-executing graph, supporting append-only-writes to update quantum and dataset status.
This graph is optimized for fast reads of its current progress and the overall graph structure, so a provenance-gathering process can quickly pick up where a previous one left off.
It is also designed to be transformed quickly into the final provenance graph.
Unlike the quantum-only predicted graph, the aggregation graph data structure models a bipartite directed acyclic graph, in which quanta and datasets are different kinds of nodes and each edge connects a dataset to a quantum or a quantum to a dataset.

Instead of a single ``zip`` archive, aggregation quantum graphs are stored as directories that can for the most part be "zipped up" to form the final read-only provenance graph.
This lets us efficiently append to multiple files (or write new chunks for multiple conceptual files).
Operations on aggregation graphs also require reading components of the predicted graph or wire graph they were constructed from.

The on-disk components of an aggregation quantum graph are described in :ref:`aggregation-components`.

.. _aggregation-components:

.. list-table:: Aggregation Quantum Graph Components
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - ``header``
     - single-block
     - Format version, collection metadata, quantum and dataset counts.
   * - ``bipartite_edges``
     - single-block
     - Edge list for a dataset-quantum-dataset graph  (using internal integer IDs).
   * - ``quanta``
     - multi-block
     - | UUIDs, task labels, data IDs, status, and exception information for
       | each quantum; log and metadata dataset UUIDs and addresses (into
       | the ``logs`` and ``metadata`` components).
   * - ``datasets``
     - multi-block
     - | UUIDs, dataset type names, data IDs, and existence status for each
       | each dataset (except log and metadata datasets).
   * - ``logs``
     - multi-block
     - The content of all butler log datasets (one dataset per block).
   * - ``metadata``
     - multi-block
     - The content of all butler metadata datasets (one dataset per block).
   * - ``quantum_addresses``
     - address (arbitrary order)
     - | UUID to integer ID mapping for all quanta; byte offsets and sizes
       | for the ``quanta``, ``logs``, and ``metadata`` components.
   * - ``dataset_addresses``
     - address (arbitrary order)
     - | UUID to integer ID mapping for all datasets; byte offsets and sizes
       | for the ``datasets`` component.

When the aggregation graph is first constructed, the ``header`` and ``bipartite_edges`` components are constructed in full from the information in the predicted or wire graph immediately.
The other components are appended to as quanta are aggregated:

1. When a set of quanta is recognized as having finished (successfully or unsuccessfully), either by receipt of a message or by scanning for the metadata and log datasets of unblocked quanta, their predicted-quantum information is loaded from the original predicted or wire graph.
2. When metadata datasets exist (i.e. the quantum was successful), they are read in full to retrieve status information and dataset provenance.  For quanta that failed (the log exists but metadata does not, or a flag from the user indicates that all unsuccessful quanta should now be considered failures), the existence of all predicted output datasets is checked.
3. Information about output datasets (including those that were predicted but not produced, but not including metadata and logs) is appended to the ``datasets`` multi-block file.
4. The metadata datasets are reconverted to JSON, compressed, and appended to the ``metadata`` multi-block file.
5. The log datasets are loaded, rewritten to JSON, compressed, and appended to the ``log`` multi-block file.
6. If any quantum's metadata and log datasets are not inputs to any downstream quantum, they are deleted from their original location.  Similarly, if any of these quantum are the last consumers of an upstream metadata or log dataset, those upstream datasets are deleted from their original locations.
7. Status and other per-quantum provenance is appended to the ``quanta`` multi-block file.
8. Existing output datasets (not including metadata and logs) are ingested into the butler database.
9. Addresses into the ``datasets`` multi-block file are appended to the ``dataset_addresses`` file.
10. Addresses into the ``quanta``, ``metadata``, and ``logs`` multi-block files are appended to the ``quantum_addresses`` file.

Most of the above steps can happen on many quanta in parallel (scanning, dataset reads, generating and compressing JSON), with a sequence point at the end to perform all file append, delete, and database insert operations in bulk from a single process/thread.
A working prototype developed for this technote uses :class:`concurrent.futures.ProcessPoolExecutor` and :mod:`asyncio` for this.
The group size could be throttled by a timer and/or dataset/quantum count minimums.

When the aggregation tool is initialized from a stored aggregation graph, it performs the following steps:

1. It reads the ``bipartite_edges`` component and constructs a lightweight in-memory bipartite DAG for the complete predicted graph.
2. It reads the ``quantum_addresses`` and ``dataset_addresses`` files in full.
3. It checks that each multi-block file has the size predicted by the corresponding address file (or, alternately, that the last block in each file is at the right location, and records the right size internally).
   An inconsistency indicates a previous interruption that must be reconciled by reading any blocks from multi-block files that are not referenced by their address files in order to recover those addresses.
   Certain failures may require these orphaned metadata blocks to be decompressed and parsed as well, in order to reconstruct the information that is to be stored in other files.
4. It checks that the ``dataset_addresses`` file has all output datasets predicted for the last quantum in the ``quantum_addresses`` file.
   An inconsistency here also indicates a previous interruption that must be reconciled.
5. If an interruption occurred, any existing output datasets that may or may not have been inserted into the database are flagged as needing special ``ON CONFLICT IGNORE`` handling on ingestion (which we would otherwise like to avoid in order to reduce locking).
6. The in-memory DAG is traversed to find the last-aggregated quanta and identify those that may be ready for aggregation soon.

Note that it is not necessary to read any of the multi-block files in full to restart processing, though it may be necessary to read the last chunk when the last attempt was interrupted.
It is necessary to avoid races when appending to the aggregation graph, either by limiting how the aggregation tool is invoked for a particular run (see :ref:`invoking-aggregation`) or utilizing some kind of locking.

The same size-consistency check information used to detect failures can similarly be used to allow multiple concurrent readers of an aggregation graph, even if writes are ongoing.
If aggregation is invoked frequently enough, this could provide an efficient way for users or services to get (slightly old) status information about an ongoing run.

.. _provenance-graphs:

Provenance Quantum Graphs
--------------------------
A provenance graph is used to store the input-output relationships and quantum status information for a full :attr:`~lsst.daf.butler.CollectionType.RUN` that is no longer being updated, either because it is complete or because it was abandoned.

Provenance graphs are constructed by extracting components from both the original predicted or wire graph zip file and an aggregation graph directory and zipping them together.
These components are copied without any changes, with the exception of the  ``dataset_addresses`` and ``quantum_addresses`` files from the aggregation graph, which must first be rewritten to be sorted by UUID to enable fast lookups by UUID in the future.

The on-disk components of provenance quantum graph are described in :ref:`provenance-components`.

.. _provenance-components:

.. list-table:: Provenance Quantum Graph Components
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - ``header``
     - single-block
     - Format version, collection metadata, quantum and dataset counts.
   * - ``pipeline_graph``
     - single-block
     - | Serialized :class:`~pipeline_graph.PipelineGraph`, including
       | all configuration.
   * - ``bipartite_edges``
     - single-block
     - Edge list for a dataset-quantum-dataset graph.
   * - ``quanta``
     - multi-block
     - | UUIDs, task labels, data IDs, status, and exception information for
       | each quantum; log and metadata dataset UUIDs and addresses (into
       | the ``logs`` and ``metadata`` components).
   * - ``datasets``
     - multi-block
     - | UUIDs, dataset type names, data IDs, and existence status for each
       | each dataset (except log and metadata datasets).
   * - ``logs``
     - multi-block
     - The content of all butler log datasets (one dataset per block).
   * - ``metadata``
     - multi-block
     - The content of all butler metadata datasets (one dataset per block).
   * - ``quantum_addresses``
     - address (sorted)
     - | UUID to integer ID mapping for all quanta; byte offsets and sizes
       | for the ``quanta``, ``logs``, and ``metadata`` components.
   * - ``dataset_addresses``
     - address (sorted)
     - | UUID to integer ID mapping for all datasets; byte offsets and sizes
       | for the ``datasets`` component.

Provenance graphs are designed to be ingested into the butler as a single datastore artifact that backs multiple datasets using a sophisticated :class:`~lsst.daf.butler.Formatter`:

``run_provenance``
  A thin wrapper around a :class:`networkx.MultiDiGraph` with quantum and dataset UUID keys, as a dataset type with no dimensions and hence a single instance for each :attr:`~lsst.daf.butler.CollectionType.RUN` collection.
  This is primarily backed by the ``bipartite_edges``, ``quantum_addresses``, and ``dataset_addresses`` components, but an extensive suite of storage class parameters would be used to add extra information to some or all graph nodes (e.g. quantum status, dataset existence, even logs or metadata) from other components.
  Higher-level provenance-query functionality would largely be implemented on top of parameter-modified reads of this dataset.

``run_provenance.metadata``
  A dictionary of basic information about the run, including submission host, begin and end timestamps (for the run as a whole), submitting user(s), etc.

``run_provenance.pipeline_graph``
  A :class:`~pipeline_graph.PipelineGraph` instance.
  This includes the content of all per-task ``_config_`` datasets and should ultimately supersede them.
  Making this a component dataset cuts down (slightly) on the number of butler dataset entries needed for provenance.

``{task}_provenance``
  A simple dataclass that combines quantum status information and optionally dataset existence information, logs, and metadata for a single quantum (as separate components), with a different dataset type for each task label (and the dimensions of that task).
  The UUID of these datasets are always exactly the UUIDs of their quanta.
  While the content in these datasets is available via ``run_provenance`` with the right parameters, having a per-quantum dataset allows provenance queries that start with quantum data ID information to leverage the general butler query system.
  This dataset would be inserted for both successes and failures (cases where any output was written, including logs), but not quanta that were never even attempted (or failed hard without writing anything, since we can't tell the difference between these and unattempted quanta without extra information from a workflow system).

``{task}_metadata``
  The original task metadata dataset type, now with storage backed by the provenance graph instead of individual files.
  We may eventually be able to retire this dataset type in favor of ``{task}_provenance.metadata``, which would cut down on the number of butler datasets in the registry, possibly significantly.
  We would have to be careful with tasks that consume upstream metadata as inputs, however, as in a predicted quantum graph a metadata dataset with the same UUID as a quantum could be problematic.

``{task}_log``
  The original task log dataset type, now with storage backed by the provenance graph instead of individual files.
  As with metadata datasets, we may eventually be able to retire this dataset type in favor of ``{task}_provenance.log``.

.. _invoking-aggregation:

Invoking Aggregation
=================================

TODO

References
==========


.. bibliography::
