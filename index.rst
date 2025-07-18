######################################################################
Reimagining quantum graphs for post-execution transfers and provenance
######################################################################

.. abstract::

   This technote proposes new approaches for ingesting pipeline execution outputs into butler repositories, with two distinct goals: storing input-output provenance in butler repositories (in both prompt processing and batch), and speeding up (and probably spreading out) the work of ingesting batch-processing outputs into the butler database.
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
To address this, some of the new quantum graph data structures here are designed to be updated in-place on disk, to reflect completed or failed quanta and existence-checked dataset artifacts.

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

Eventually, we will want direct ``pipetask`` executions to operate the same way as BPS (in this and many other ways), but with all steps handled by a single Python command.
This may be rolled out well after the batch and prompt-processing variants are, especially if provenance-hostile developer use cases for clobbering and restarting with different versions and configuration prove difficult to reconcile with having a single post-execution graph dataset for each :attr:`~lsst.daf.butler.CollectionType.RUN` collection.

This technote partially supersedes :cite:`DMTN-205`, which assumes input-output graph provenance would be stored in butler database tables rather than files, but otherwise discusses provenance requirements and queries more completely.
We now prefer the file approach for three reasons:

- It is simpler to implement.
- Provenance changes are much easier.
- It provides a better separation of concerns between ``daf_butler`` and ``pipe_base``.

This change does not affect our ability to support provenance queries (and in particular, we can continue to support the full IVOA provenance model).
But it may limit our ability to use external tooling to *query* the IVOA provenance representation (e.g. of those tools expect a SQL database with a schema that hews closely to that representation).
It may also add some overheads (involving shallow reads of files that correspond to a full :attr:`~lsst.daf.butler.CollectionType.RUN` collection) to some operations.
But we expect this to be a worst-case slowdown (and a small one); we have no reason to believe that file-based provenance would be slower in general.

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

In most of the new quantum graph file formats proposed here, we will similarly rely on compressed JSON indexed by byte offsets, with some important changes.
Instead of using a custom binary header and high-level file layout, we will use ``zip`` files to more easily aggregate multiple components (i.e. as "files" in a ``zip`` archive) that may be common to some graph variants but absent from others.
Each JSON component will be represented by a Pydantic model, and we will use ZStandard\ [#zstd-in-zip]_ for compression.
Pydantic representations of butler objects (e.g. :class:`~lsst.daf.butler.DatasetRef`) will be deliberately independent of the ones in ``daf_butler`` to avoid coupling the ``RemoteButler`` API model versions to the quantum graph serialization model version, except for types with schemas dependent on butler configuration (dimension and datastore records).

Unfortunately, while the ``zip`` format is great for aggregating a small number of files (or even thousands), the overheads involved in storing millions of files can be significant compared to the simpler, more lightweight byte-offset indexing used in the current :class:`QuantumGraph` files, in terms of both file size and partial-read times.
As a result we plan to continue to store some per-quantum and per-dataset information in "multi-block" files containing directly-concatenated blocks of compressed JSON.
To improve our ability to handle hard failures and file corruption, each block in a multi-block is preceded by its own (post-compression) size.

Each multi-block "file" is associated with a separate address "file" (i.e. in the same ``zip`` archive), which is a simple binary table in which each row holds a UUID, an internal integer ID, one or more byte offsets, and one or more byte counts (a single address file can index multiple multi-block files).
Address files can be sorted by UUID, which yields a typical-case O(1) lookup performance (since UUIDs are more or less draw from a uniform random distribution), with even the worst-case only O(log N).
As in the current quantum graph format, this will allow us to use these IDs instead of UUIDs in certain other components, making them compress much better.

A predicted or provenance quantum graph file will thus be a single ``zip`` file containing several simple "single-block" compressed-JSON header files, one or two address files (for quanta and possibly datasets) and one or more multi-block files.
While we will not attempt to hide the fact that these are ``zip`` files and very much intend to leverage that fact for debugging, we will continue to use a custom filename extension and do not plan to support third-party readers.

The update-in-place aggregation graphs will contain similar compressed-JSON components, but will combine them via blob columns in a SQLite database instead of ``zip`` archives and multi-block files.
This will give us transactional writes, which will be important for fault tolerance, and enough efficient partial-read support to allow interrupted aggregation processes to resume quickly.
By using the same compressed JSON components, the conversions between an aggregation graph and its predecessor and successor graphs should still be quite fast.

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
     - | Serialized :class:`~pipeline_graph.PipelineGraph`, including all
       | configuration.  Lazy-loads config classes and other import-dependent
       | types, allowing them to be inspected even after code changes.
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

An "aggregation" graph is used to represent the provenance of a currently-executing graph, supporting update-in-place writes to update quantum and dataset status.
Unlike the other (read-only) graph variants, aggregation graphs are written to SQLite files in order to enable transactional writes and optimize certain partial reads that are needed to resume quickly after interruption.
It is also designed to be transformed quickly into the final provenance graph.
Unlike the quantum-only predicted graph, the aggregation graph data structure models a bipartite directed acyclic graph, in which quanta and datasets are different kinds of nodes and each edge connects a dataset to a quantum or a quantum to a dataset.

The aggregation graph SQLite database will have the following tables:

- ``header``: a single-column single-row compressed-JSON blob with the format version, collection metadata, and quantum and dataset counts.
- ``bipartite_edges``: a single-column single-row compressed-JSON blob with an edge list for the dataset-quantum-dataset graph (using internal integer IDs).
- ``quantum``: per-quantum provenance.  See :ref:`aggregation-quanta-table`
- ``dataset``: per-dataset provenance.  See :ref:`aggregation-dataset-table`.
- ``to_ingest``: the internal integer IDs of datasets that are present in the ``datasets`` table but not yet ingested into the central butler database.
- ``to_delete_logs``: the internal integer quantum IDs whose logs should be deleted but have not yet been.
- ``to_delete_metadata``: the internal integer quantum IDs whose logs should be deleted but have not yet been.

Operations on aggregation graphs also require reading some components of the original predicted quantum graph.

.. _aggregation-quanta-table:

.. list-table:: Aggregation Quantum Graph ``quanta`` schema
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - quantum_index
     - INTEGER PRIMARY KEY
     - Internal integer ID.
   * - quantum_uuid
     - UUID
     - External UUID.
   * - succeeded
     - BOOLEAN
     - Whether this quantum succeeeded.
   * - provenance
     - BLOB
     - Task labels, data IDs, status, and exception information for each quantum.
   * - log_index
     - INTEGER
     - Internal integer ID for the associated log dataset.
   * - log_data
     - BLOB
     - Actual content of the log dataset.
   * - metadata_index
     - INTEGER
     - Internal integer ID for the associated metadata dataset.
   * - metadata_data
     - BLOB
     - Actual content of the metadata dataset.

.. _aggregation-dataset-table:

.. list-table:: Aggregation Quantum Graph ``dataset`` schema
   :header-rows: 1

   * - Name
     - Type
     - Description
   * - dataset_index
     - INTEGER PRIMARY KEY
     - Internal integer ID.
   * - dataset_uuid
     - UUID
     - External UUID.
   * - provenance
     - BLOB
     - Dataset type names, data IDs, and existence status for each dataset.

When the aggregation graph is first constructed, the ``header`` and ``bipartite_edges`` tables are constructed in full from the information in the predicted or wire graph immediately.
The other tables are appended to as quanta are aggregated.
The aggregation tool can be run in two different modes:

- In "monitor" mode, the tool only processes successfully-completed quanta, as indicated by the existence of the ``metadata`` dataset.
  This allows failures to be recovered by retries and restarts without invalidating any aggregation work already completed.

- In "finalize" mode, the tool processes both successful and failed quanta, and when all quanta have been processed, it transforms the aggregation graph into a :ref:`provenance quantum graph <provenance-graphs>` and ingests it into the butler.

The tool will run in parallel using Python ``multiprocessing``, with a single process responsible for writing to the SQLite aggregation graph file and the central butler database.
This process will batch the writes for many quanta into a single transaction based on a timer and/or quantum/dataset counts.
Other worker processes will be responsible for scanning for and reading ``metadata`` and ``log`` files, extracting information from them into provenance Pydantic models, and transforming that into compressed JSON to send to the writer process.
In detail, within one transaction:

1. When a quantum is recognized as having finished (successfully or unsuccessfully), either by receipt of a message or by scanning for the metadata and log datasets of unblocked quanta, its predicted-quantum information is loaded from the original predicted or wire graph.
2. When metadata datasets exist (i.e. the quantum was successful), they are read in full to retrieve status information and dataset provenance.
   For quanta that failed (the log exists but metadata does not, or a flag from the user indicates that all unsuccessful quanta should now be considered failures), the existence of all predicted output datasets is checked.
3. Information about output datasets (including those that were predicted but not produced), is appended to the ``dataset`` table.
4. The IDs of datasets that were actually produced are added to the ``to_ingest`` table.
5. Information about the quantum (including the full content of the log and metadata files) is appended to the ``quantum`` table.
6. If the quantum's metadata and log datasets are not inputs to any downstream quantum, they are scheduled for deletion from their original location by inserting their IDs into the ``to_delete_logs`` and ``to_delete_metadata`` tables.
   Similarly, if this quantum is the last consumers of an upstream metadata or log dataset, those upstream datasets are also scheduled for deletion.

On a possibly different cadences, the tool also:

1. Inserts all ``to_ingest`` datasets into the central butler database (without a SQLite transaction open, but with ``ON CONFLICT IGNORE`` to silently skip datasets that may have been ingested already), and then clears that table.
2. Optionally copies the SQLite file to a permanent location as a checkpoint, if it is currently in a temporary one (e.g. in environments where only permanent object storage is available, and per-job scratch has to be used for SQLite).
3. Actually deletes all metadata and log metadata files that are scheduled for deletion, and then clears that table.
   If and only if permanent storage checkpoints are needed, this should only happen just after a checkpoint has been created and block any new logs or metadata from being scheduled for deletion (via a SQLite transaction) until it is done.
   This ensures that we always have a copy of any log and metadata content either in the checkpoint location or in their original locations.

When the aggregation tool is initialized from a stored aggregation graph, it performs the following steps it reads the ``header`` and ``bipartite_edges`` components in full and constructs a lightweight in-memory bipartite DAG for the complete predicted graph.
It then queries the quantum table for the IDs of the quantum that have already been aggregation and annotates the graph accordingly, allowing a graph traversal to identify unblocked quanta that should be monitored for completion.

Note that it is not necessary to read any of the BLOB files in full to restart processing.
While SQLite can in principle handle concurrent writes from multiple processes, it does not do so efficiently, and it is better to avoid races when appending to the aggregation graph by limiting how the aggregation tool is invoked for a particular run (see :ref:`invoking-aggregation`).
It may be possible to configure SQLite to support parallel reads from other processes without significantly slowing down the writer process.
If aggregation is invoked frequently enough, this could provide an efficient way for users or services to get (slightly old) status information about an ongoing run.

.. _provenance-graphs:

Provenance Quantum Graphs
--------------------------
A provenance graph is used to store the input-output relationships and quantum status information for a full :attr:`~lsst.daf.butler.CollectionType.RUN` that is no longer being updated, either because it is complete or because it was abandoned.

Provenance graphs are constructed by extracting components from both the original predicted or wire graph zip file and an aggregation graph SQLite file and zipping them together.
These components are copied without any changes, with the exception of the  ``dataset_addresses`` and ``quantum_addresses`` files from the aggregation graph, which must first be generated from the columns in the aggregation graph tables.

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
     - | Serialized :class:`~pipeline_graph.PipelineGraph`, including all
       | configuration.  Lazy-loads config classes and other import-dependent
       | types, allowing them to be inspected even after code changes.
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
====================

Our goal thus far has been to design low-level tooling that would be usable in many different schemes for how and when it could be invoked, but it is also worth exploring how various schemes might work in a bit more detail.
Because the aggregation graph data structure does not support concurrent updates, any particular batch workflow would have to stick to one scheme, but different workflows could use different schemes, depending on their needs and scale.
Introducing a locking mechanism would also permit mixing different schemes within a single workflow.

Dependent DAG Jobs
------------------

Instead of making all aggregation the responsibility of a single ``finalJob``, we could add multiple aggregation jobs throughout the workflow DAG.
These jobs would have to be placed in the DAG such that they do not run in parallel with each other, and they'd need depend on regular jobs to space them out temporally (e.g. we could have one aggregation job for each task).
In this scheme each aggregation job would look only for the quanta it depends on in the workflow DAG, and then exit.
A regular ``finalJob`` would also be included to take care of any aggregation not done by the previous jobs, as well as constructing and ingesting the final provenance graph.
Most aggregation jobs would run in "monitor" mode, and the ``finalJob`` would run in "finalize" mode.

A major disadvantage of this scheme is that each aggregation job would only get executed if all of its upstream jobs actually succeeded; running a job regardless of upstream success or failure as in ``finalJob`` may or may not be possible for different WMSs, and it would at the very least require additional BPS work.
An early failure could thus prevent all aggregation jobs from ever becoming unblocked, leaving too much work for ``finalJob``.
While this would still be an improvement on the status quo (since quanta usually succeed, and failures also block outputs from being produced), it would likely not fully solve the problem of slow ``finalJob`` performance.

Single Monitor Job
------------------

Because we envision an aggregation tool that could be configured to alternatively sleep and poll for newly-completed quanta (via metadata and log existence checks, targeting only the unblocked quanta), we could delegate all aggregation work to a single workflow DAG job that is launched very early in the processing (e.g. after ``pipetaskInit``).
For fault tolerance, this job would need to be configured to automatically retry indefinitely (unless explicitly killed with ``bps cancel``), and it would complete successfully only when all predicted quanta are aggregated (whether successful or not).
We would also need to find a way to ensure it is scheduled for execution early rather than late, since it would "tie" with essentially all regular jobs in any purely topological ordering of the DAG.

A regular ``finalJob`` would also be included, with a dependency on the long-lived aggregation job as well as the regular jobs, but in the usual case it would only be responsible for constructing and ingesting the provenance graph from a fully-populated aggregation graph.

The main disadvantage of this scheme is that using existence checks to poll for metadata and log datasets for completed results in more load on the storage system.
WMS-specific tooling wrapped by BPS might be able to do this more efficiently.
If not, load could still be mitigated by increasing the fraction of the time the job spends sleeping, and possibly ensuring that these jobs go to a special queue that throttles their overall activity (including overall database insert load).

Assuming the special batch-scheduling behavior needed for this job is not difficult to implement in BPS, it probably provides the best balance between load-control and ease of implementation.

User-space Daemon
-----------------

It is not uncommon for BPS users to run ``bps report`` repeatedly while their workflows are running.
This suggests that users might appreciate being able to just run the aggregation tool (in sleep/poll) mode themselves, since the tool would thus naturally be able to provide them with the status of both the execution and the ingestion of outputs into the butler (with former slightly out-of-date).
It is also much easier to diagnose problems in aggregation when they occur directly in a user's interactive shell rather than a batch worker.

This scheme would be incompatible with having a ``finalJob`` that also runs aggregation (since we don't want parallel writes to the SQLite aggregation graph file), so it would rely on users remembering to actually run the tool and keeping it running over the course of a workflow.
As with a dedicated DAG job, load from existence-check polling may also be a concern if the sleep fraction is not high enough, and in this case there would be no coordinated way to throttle multiple workflows from multiple users.

Given these disadvantages, this would probably not be our preferred long-term approach for production jobs, but it would be extremely useful to have available for development and debugging.

Multi-workflow Aggregation Service
----------------------------------

To provide the most control over how database inserts are batched together and fully minimize existence checks, we could stand up a data-facility-wide service that receives messages (e.g. Kafka) from completed batch jobs to trigger aggregation.
Such a service would need to be notified when a new submission is started, allowing it to set up a new aggregation graph for it, and it would append to each graph only when either a certain number of quanta completed or a certain amount of time had passed.
It would need to provide ways for users to cancel active workflows and determine when a workflow's outputs had been fully aggregated and ingested into the butler.

The main downside of this approach is that it is significantly more complex in code development and especially deployment than other options.
Even if we go this route at USDF or other major data facilities, we should provide at least one alternative for smaller-scale BPS deployments.

Handling Workflow Cancels and Restarts
======================================

In any of the above schemes, at least some of the quanta and output datasets produced by a workflow are likely to have already been aggregated (and ingested into the central butler database) when a workflow is canceled.

For invocation schemes that involve a ``finalJob`` that runs aggregation, we have three choices for what cancelation should do, with different implications for how a subsequent restart might work:

- If we do not run ``finalJob`` at all, the run is left partially-aggregated, with a SQLite aggregation graph still present that can be used to efficiently continue aggregation in a restart or by direct user invocation of the aggregation tool.
  This increases the likelihood of leaving output datasets "orphaned" in storage with no records in the butler database (unless we implement :ref:`butler-managed-qgs`), if aggregation is never finalized.
  Workflow restarts using the same predicted graph and in-progress aggregation graph would require no special handling in this mode.

- If we run ``finalJob`` in "monitor" mode, the datasets produced by successful quanta will not be orphaned, but those produced by failures (mostly logs) could be, if aggregation is not finalized later.
  Once again workflows restarts would not require any special handling.
  Note that switching to "monitor" mode on cancelation would require some way of passing the canceling user's intent to the ``finalJob``, which may not be possible (or desirable if it is too messy).

- If we run ``finalJob`` in "finalize" mode to completion, no datasets can be orphaned, but restarts get more difficult: we would need to remove the provenance graph from the butler repository and transform it back into an aggregation graph (this should be fairly efficient; all of the necessary information is present in roughly the right form).
  This transformation would have to drop the provenance entries for all failures, since the restart implies that at least some of them may become successes.
  Log and metadata datasets that are needed as inputs to failed quanta would also have to be restored to their original single-file locations.
  While we do not do this today, it would also probably be appropriate to delete all outputs of failed quanta from at least the butler database at this time, to reduce reliance on clobbering mechanics and avoid the appearance of any butler-managed dataset being updated in place.

.. _future-extensions:

Future Extensions
=================

High-level Provenance Queries
-----------------------------

This technote focuses on how to store provenance in the butler despite the fact that the core concepts of that provenance (the :class:`PipelineTask` ecosystem) are defined in packages downstream of butler: by ingesting butler datasets whose storage classes have parameters and components that provide enough flexibility to back a powerful and efficient provenance-query system.
Designing that query system is an important part of the overall butler provenance framework, but is out of scope here.

A few cases are worth calling attention to:

- By including all predicted datasets in the provenance files along with their existence status *at the time of execution*, we make it possible to whether a dataset was or was not not available as an input to a downstream task, regardless of whether it was ultimately retained.

- Full-but-shallow reads of the bipartite graph will make it easy to support finding the set of indirect input datasets of a particular type that contributed to a particular output dataset (e.g. the ``raw`` datasets that went into a ``coadd``).
  These sorts of questions need to be posed with care for which edges to traverse, however; e.g. *all* ``raw`` datasets in a DR contribute to the global photometric calibration, so naively they all contribute to any coadd dataset anywhere on the sky, at least as far as the full DAG is concerned.

- A predicted quantum graph can be built for exactly reconstructing any existing dataset, provided we can determine the :attr:`~lsst.daf.butler.CollectionType.RUN` collection it was originally built in.
  We can find that given the UUID for any dataset we have retained (by querying the butler database), and we can find it via provenance queries on other retained datasets.
  But we do not have a good way to find the :attr:`~lsst.daf.butler.CollectionType.RUN` collection from the UUID of a non-retained dataset.
  Given that the only way we expect users to obtain UUIDs of non-retained datasets is by inspecting the provenance of a retained one (either via provenance queries or file metadata), we do not expect this to be a problem.

Additional Small-File Aggregation and Purging
---------------------------------------------

We propose including the small ``_metadata`` and ``_log`` datasets into the large per-run provenance files (instead of into separate ``zip`` files, as has been previously proposed) for two reasons:

- We need to at least existence-check (``_log``) and maybe read (``_metadata``) these datasets in order to gather provenance, and we don't want to end up doing that twice;
- As noted in :ref:`file-formats`, at the scale of millions of files, direct ``zip`` performance is not nearly as fast and space-efficient as our addressed multi-block storage.

It may be useful in the future to zip together other output datasets at the aggregation stage, and this could also be a good time to delete intermediate datasets that we know we don't want to bring home.
The aggregation system is in a good position to determine when it is safe to do either, and to do it efficiently, but it would need to build on top of a currently-nonexistent system for describing how different dataset types in a pipeline should be handled.

.. _butler-managed-qgs:

Butler-Managed Quantum Graphs
-----------------------------

The fact that :class:`~lsst.daf.butler.QuantumBackedButler` writes to storage before we ingest the corresponding datasets into the butler database opens the door to orphaning those files with little or no record that they exist when failures occur.

The new quantum graph variants described here could help with this: the aggregation tooling is designed to find exactly the datasets that might exist but haven't been ingested into the butler database yet.
If we registered these graphs with the butler before we start executing them (i.e. stored them in butler-managed locations), files wouldn't ever really be fully orphaned; butler tooling could always go back and walk those graphs to ingest or delete those files later.

Because the butler intentionally doesn't know anything about quantum graphs per se, this would require a new abstraction layer for this sort of "uningested dataset manifest" for which quantum graphs could serve as one implementation.

Passing Extended Status via BPS
-------------------------------

The status-gathering described here depends only on middleware interfaces defined in ``pipe_base`` and ``ctrl_mpexec`` (just like ``pipetask report``), which provides a good baseline when nothing else about the execution system can be assumed.
BPS generally knows at least a bit more, however (especially about failures), and some plugins may know considerably more (especially if we implement per-job status reporting).
In the future we should provide hooks for BPS plugins to feed additional status information to the aggregation tooling.

References
==========

.. bibliography::
