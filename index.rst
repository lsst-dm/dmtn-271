:tocdepth: 1

.. sectnum::

.. Metadata such as the title, authors, and description are set in metadata.yaml

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

Abstract
========

This technote proposes a new model for executing pipelines against butler repositories, in which quantum graphs are stored in the butler repository and the `pipetask` tool is ultimately replaced with a new command-line interface that uses a Quantum-backer for all execution.

Introduction and related documents
==================================

The design proposal here is in many respects a natural extension to :cite:`DMTN-177`, which addresses the database-contention problem with batch execution of pipelines against a SQL-backed butler client, by introducing a "quantum-backed butler" (QBB) that is backed by a quantum graph file.
This gives batch execution via BPS a sequence of operations that is *almost* transactional:

1. "Begin" the run by generating a quantum graph and other downstream DAGs.

2. "Execute" the run by calling `PipelineTask`` code, with file artifacts written to butler-managed locations.

3. "Commit" the run by inserting metadata about the already-written file artifacts to the central butler database.

The primary change proposed here is to make the first step a butler *write* operation, in which the quantum graph is saved to a location managed by the butler, in a format abstracted by the butler, but not as a simple datastore dataset; because quantum graph content provides metadata about file artifacts (similar to the role played by a SQL registry), we envision quantum graph storage to be managed by a new third butler component, which we will call the "workspace" (at least for now; naming is hard).
One advantage of butler management of quantum graphs is that it would allow us to implement the missing piece of this execution model, "rollback" (deleting file artifacts instead of updating the database).  This would be a very useful for some development (as opposed to production) use cases, where the goal is to just get a `PipelineTask` working.

While the QBB execution model (or something similar) is necessary for at-scale batch execution, it's also probably a more useful and intuitive interface for *all* execution, especially with rollback included, as the `pipetask` tool's current behavior w.r.t. collection chaining and failure recovery is often confusing to users.
Having a single straightforward execution model (and interface) for both BPS and non-batch tools has also long been a goal for obvious reasons.
Note that this is by definition a complete overhaul of the `pipetask` interface, and throughout this technote we will assume the transition will be best handled by introducing a new command-line interface via new `butler` subcommands, while `pipetask` as a whole is deprecated.
A new command-line tool separate from `butler` would also be perfectly viable; our preference for `butler` subcommands is in no small part an attempt to avoid getting distracted by naming questions.

Unfortunately this execution model doesn't really fit well with the butler's current consistency model, in which a dataset is permitted to be present in  a datastore only if it is known to the associated registry - we explicitly and intentionally violate this rule during QBB execution before the "commit" occurs.
Reworking the consistency model to permit QBB execution (and solve some other problems) is the primary goal of :cite:`DMTN-249`, by introducing a "journal file" concept for tracking datasets that have file artifacts but no central database records; this technote fleshes out the assertion in :cite:`DMTN-249` that a file or files representing a quantum graph would typically play that role during execution, with a necessary piece of this being the quantum graph being considered a part of the data repository.

The other major motivation for this technote is the need to implement some amount of butler quantum provenance.
:cite:`DMTN-205` describes a schema for storing provenance the central database after execution is complete, which can be viewed as one possible way of storing quantum graph content in the central repository after an "execution transaction" has been committed.
File-based storage is also an option, at least for some kinds of provenance queries, this is something we expect the butler interfaces to abstract over.
In addition, the provenance picture is greatly simplified if the *predicted* quantum graph is always managed by butler instead of being left to the user; for the most rigorous provenance we want butler to have complete chain of custody of the graph from its inception.

.. _pipeline-execution-model:

Pipeline execution model
========================

Overview
--------

We are focused on satisfying three primary pipeline execution use cases:

- Batch production, in which software and configuration is *mostly* frozen, and ``RUN`` collections are the natural atomic unit: there is one quantum graph, one batch workflow, and one transfer job for each ``RUN`` collection.
  Each of these RUN collections typically corresponds to a shard (one of several disjoint sets of data IDs that together span the full campaign) of a step (one of several disjoint subgraphs of the full pipeline).

- Prompt processing, in which software and configuration are also frozen, but there is one ``RUN`` collection (typically) for each night of observing, with a new *logical* quantum graph for each observation (typically a ``visit`` or ``{visit, detector}`` combination), and it may not even be possible to instantiate each *logical* quantum graph up front, because of the need to start each step of processing as soon as possible.

- Pipeline software development, in which software and configuration are very much in flux, but the output data products don't need to be retained (at least not in the long term) and hence provenance loss is acceptable.

Only the first of these is fully compatible with the approximate begin/execute/commit sequence used already by BPS with QBB, in which quantum graph generation represents the "begin" step and each ``RUN`` collection is more or less atomic.
Prompt processing requires support for executing and committing results for some data IDs (e.g. one visit) before executing and committing those of another (e.g. the next visit), all within the same ``RUN`` collection.
And software development use cases prevent us from (always) freezing software and pipeline definitions at transaction start, let alone quantities derived from them, like the quantum graph.
But this isn't actually a problem, as these do not present any obstacle to the important conceptual change here, which is that records representing output datasets are only inserted into the central database after all quanta in a graph have been processed, but these datasets are tracked in other ways prior to this and are fully managed by the butler throughout.

Our execution model will instead define its "begin" analog to be the creation of a new *workspace collection*, with initial datasets describing only the pipeline, software versions, and input collections.
Workspace collections are not queryable until they are committed, which turns them into ``RUN`` collections.

Our "execute" stage will be split several incremental steps:

- declaring the tasks from the already-provided pipeline that will actually be run (and writing their init-output datasets);
- building the quantum graph, by providing data ID constraints;
- actually running tasks.

These generally have to be performed in this order, but some exceptions are quite useful and should not be hard to implement.

The "commit" stage is simple: the workspace collection is transformed into a ``RUN`` collection, and the records for its datasets (both registry and datastore) are inserted into the central database.
A CHAINED collection may optionally be created or modified to include the new ``RUN`` collection at the same time.

The "rollback" stage is even simpler: the workspace collection and all of its contents are deleted.

It should be emphasized that these are *conceptual* stages that are most meaningful for lower-level code and reasoning about data repository consistency.
For convenience, high-level interfaces will often permit many steps to be performed together (see :ref:`user-interface`).

External workspaces
-------------------

We actually have a fourth use case we'd like to support: science users running pipelines in the Rubin Science Platform.
We don't have a good sense for what storage systems these users would be writing file artifacts to, but we *do* want to limit the degree to which the client-server butler needs to support direct execution.

With that in mind, we envision permitting a workspace collection to be created its own datastore root in a location controlled by the user.
This opens up two options for committing the workspace:

- output file artifacts could be transferred back to the central data repository;
- a new (typically SQLite) data repository could be created from the workspace collection's contents in the external workspace's root.

The latter is a limited form of data repository chaining, with the main limitations being:

- input datasets from the central data repository could at best be ingested into the new repo as external artifacts (i.e. with absolute URIs), and even then only if the central database can promise that they will never be deleted over the lifetime of the satellite data repository;
- it is not clear we can support queries or quantum-graph generation for subsequent processing that requires inputs from both the satellite and the central data repository - this is the classic registry-chaining problem, and while I am more optimistic about it than I was in the past, it is not something we want to promise right now.

External workspaces should be implemented such that "rollback" is exactly the same as deleting the directory containing the workspace; users *will* do this and it will be easier for all involved if we make it a well-defined, legal operation rather than something that involves any kind of administrative recovery.

Sharding
--------

To support the prompt-processing use case, we need to permit quantum-graph generation to be run multiple times on multiple disjoint sets of data IDs, and allow each of these sets of data IDs to be committed separately.
To do this, a workspace collection may be initialized with a set of *sharding dimensions*, which will restrict the tasks that can be run in that workspace to only those for which sharding by those dimensions defines disjoint partition of all quanta, and require that all quantum-graph generation invocations provide values for those dimensions explicitly.
Sharded workspace collections may coexist with the ``RUN`` collections they eventually become for a time, but any data ID identifying the sharding dimensions is strictly in one or the other.

A workspace collection will not be queryable in the same way as a regular, completed ``RUN`` collections, which is what a workspace collection will become when it is committed; in the future we may be able to provide some limited, opt-in support for querying collections in this state, but at first most queries will not be supported at all.
The initial datasets are stored only as file artifacts, using a prediction-mode datastore, and the ``packages`` dataset is optional: not writing it will put the workspace in *development* mode, in which provenance-unfriendly operations are permitted and the provenance that is saved is marked as unreliable.
At this stage we will store the full pipeline, not just the subset of tasks being run.
Adding the workspace collection itself may involve the insertion of records into the database and/or files being written to a file-storage location defined by data repository configuration entries (see :ref:`workspace-apis-and-implementations`).

It is possible that sharding could also be useful for data-release batch production as well, but the current Campaign Management approach prefers to put *its* shards in different ``RUN`` collections anyway.
It may still be useful to define workspace collections with sharding dimensions even when each one only contains one shard (of many data IDs), simply to guard against accidentally running tasks incompatible with those sharding dimensions.

Development mode
----------------

A workspace collection may be initialized in *development mode*.
This will cause the software versions to not be written out at all, and other output datasets (including collection-wide datasets like pipeline definition datasets, configs, and task init-outputs) will frequently be overwritten.
When committed, some provenance from a development workspace collection may be dropped, and the rest will be marked as unreliable.
Note that cannot just always roll back a development workspace collection because being able to test downstream tasks (which may require making new quantum graphs in new workspaces) is an important part of development work.

.. _user-interface:

User interface
==============

.. _workspace-apis-and-implementations:

Workspace APIs and implementations
==================================

.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
