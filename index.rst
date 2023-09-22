:tocdepth: 1

.. sectnum::

.. Metadata such as the title, authors, and description are set in metadata.yaml

.. TODO: Delete the note below before merging new content to the main branch.

.. note::

   **This technote is a work-in-progress.**

Abstract
========

This technote proposes a new model for executing pipelines against butler repositories, in which both quantum graphs and output datasets are managed by a *butler workspace* before being committed back to the central data repository.
Workspaces generalize the "quantum-backed butler" system (DMTN-177 :cite:`DMTN-177`) to all butler-backed execution with a more powerful and use-case-focused interface than the current ``pipetask`` tool.
By storing quantum graphs in a butler-managed location, workspaces also pave the way to storing quantum provenance in butler.

Conceptual overview
===================

Origin in BPS/QBB
-----------------

The execution model of the Batch Production Service (BPS) with quantum-backed butler (QBB, see DMTN-177 :cite:`DMTN-177`) can be seen as three intuitive steps, analogous to transactional operations in SQL:

1. ``bps prepare`` starts the process defining a moderately large unit of processing processing, via a sequence of related directed acyclic graphs (DAGs), starting with the quantum graph.
   These graphs and other WMS-specific files are saved to a "submit" directory, whose location is managed by the user even though the contents are managed by BPS.
   This can be seen as the beginning of a transaction for a run - no modifications are made to the central data repository.

2. ``bps submit`` executes that processing by delegating to a third-party workflow management system (WMS).
   All of these batch jobs (aside from the very last) write the output datasets as file artifacts, but do not read or write from the central data repository's database at all.

3. The last batch job inserts records describing the produced file artifacts to the central database, effectively committing the transaction by putting all produced datasets into a single :attr:`~lsst.daf.butler.CollectionType.RUN` collection.

These steps are not always visible to users - ``bps submit`` can run the ``prepare`` step itself, and usually does, and in the absence of errors the division between the regular execution batch jobs and the final "transfer" job is invisible.
What matters is that this model provides a sound foundation for higher-level tools, with the existence of a discrete "commit" step the aspect that is most notably lacking in non-BPS execution.

Workspace basics
----------------

The middleware team has long wanted to unify the ``bps`` and ``pipetask`` interfaces, and the proposal here is to do just that, by introducing a new *workspace* concept that generalizes the BPS/QBB flow to all execution.
A workspace is best thought of an under-construction, uncommitted :attr:`~lsst.daf.butler.CollectionType.RUN` collection, known to the central data repository but not included in its databases (except perhaps as a lightweight record of which workspaces exist).
Users interact with a workspace via a :class:`WorkspaceButler` client, which subclasses :class:`~lsst.daf.butler.LimitedButler`.
When a workspace is committed a regular :attr:`~lsst.daf.butler.CollectionType.RUN` collection is created and the workspace ceases to exist.
Workspaces can hold quantum graphs, output file artifacts (via a datastore), and provenance/status information.

Workspaces can have multiple implementations and capabilities.
:class:`WorkspaceButler` itself will not assume anything about tasks or pipelines, as appropriate for a class defined in ``daf_butler``.
Our main focus here will be a :class:`PipelineWorkspaceButler` subclass that serves as a ``pipetask`` replacement, and we imagine it storing its persistent state (mostly quantum graphs) using files (see :ref:`implementation-notes`).
We envision BPS and its plugins delegating to the :class:`PipelineWorkspaceButler` via a command-line interface, much as they currently do with ``pipetask``, but we hope that eventually the front-end interface to BPS could be a :class:`WorkspaceButler` subclass as well.

Workspaces *may* read from the central repository database for certain operations after they are created, but they must not write to it except when committed, and it is expected that many core operations will not access the central database at all (such as actually running tasks with :meth:`PipelineWorkspaceButler.run_quanta`).

Provenance
----------

One clear advantage of the workspace model is that the "commit" step provides an opportunity for provenance to be gathered and possibly rewritten into a form more focused on post-execution reads than at-scale execution writes.
This is a problem largely ignored by DMTN-205 :cite:`DMTN-205`, which describes a central database schema for provenance but lacks a complete plan for populating it.
In addition to the workspace interfaces, this technote will describe new methods on the :class:`~lsst.daf.butler.Butler` class that provide some provenance query support.
These could be implemented by storing quantum graphs and quantum status in files after workspaces are committed, while leaving room for database storage of provenance graphs as described in DMTN-205 :cite:`DMTN-205` in the future.

Development mode
----------------

Workspaces would also be useful in pipeline software development and small-scale testing, especially if we could (sometimes) relax our consistency controls on software versions and pipeline configuration prior to "commit", a state we will call "developement mode".
Furthermore, many development runs would not be committed at all, because the data products themselves are not the real outcome of that work - working code is.
This suggests that workspaces should also support an analog of SQL's ``ROLLBACK`` as an alternative to commit (though we will use :meth:`WorkspaceButler.abandon` as the nomenclature here to avoid confusion with the very different relationship between ``commit`` and ``rollback`` in other contexts, e.g. some version control systems).

External workspaces
-------------------

If we provide an option to create a workspace in an external location instead of inside a central data repository, we get an intriguing possibility for execution in the Rubin Science Platform (RSP) or on a science user's local compute resources.
The quantum graph stored in the workspace knows about all needed input datasets, and these could be transferred to the external location as well, providing a self-contained proto-data-repository that could be manually moved and ultimately executed anywhere, and it contains all of the information needed to reconstitute a standalone data repository with its own database.
Alternatively, if the workspace is left in a location where it can read from the central data repository but not write to it, input datasets could be left in the central data repository and the user could choose to either create a standalone repository that merely references them or commit by transferring output dataset file artifacts to the cental repository root.

Sharded workspaces and Prompt Processing
----------------------------------------

We have one major use case that does not fit as well with this model, at least as described so far: prompt processing runs one ``visit`` or one ``{visit, detector}`` at a time, appending these results to a single :attr:`~lsst.daf.butler.CollectionType.RUN` collection that typically covers an entire night's processing.
The switch from ``pipetask`` to workspaces wouldn't actually *require* any major changes to the prompt processing framework's architecture, because each prompt processing worker has its own full SQL-backed butler, with the fan-out to workers and long-term persistence represented as transfers between data repositories.
Workspaces would be created and committed wholly within individual workers,  requiring some changes to low-level tooling like :class:`~lsst.ctrl.mpexec.SeparablePipelineExecutor`.
The existence of a full Python interface for workspace-based execution may actually remove the need for separate pipeline executor classes entirely.

An alternative approach in which workspaces are more tightly integrated with prompt processing is both intriguing and more disruptive.
In this model, prompt processing would have a custom :class:`WorkspaceButler` implementation that represents a client of a central-butler workspace existing on a worker node.
This would give prompt-processing complete control over quantum graph generation and how it is interleaved with execution, allowing the graph to be built incrementally as new information arrives (first the ``nextVisit`` event, then a ``raw`` file artifact, and then perhaps a ``raw`` for a second stamp), with each processing step starting as soon as possible.
There would be no SQL database on the workers at all, and the :class:`WorkspaceButler` implementation would implement its own :meth:`~lsst.daf.butler.LimitedButler.get` and :meth:`~lsst.daf.butler.LimitedButler.put` implementations for use by tasks.
This would be a natural place to put in-memory caching of datasets, while delegating to a regular :class:`~lsst.daf.butler.Datastore` when actual file I/O is desired.

This model would not work if a :attr:`~lsst.daf.butler.CollectionType.RUN` collection is the (only) unit for commits and quantum graph generation, since we would want to commit results from each worker to the central butler on a per-data ID basis.
To address this, a workspace may be created with *sharding dimensions*, which define data IDs that may be committed independently.
This restricts the tasks that may be run in such a workspace to those that have at least those dimensions, ensuring that all output datasets in the workspace can be divided into disjoint sets according to those data IDs.
Committing a data ID from a sharded workspace moves all datasets and provenance with that data ID to the corresponding :attr:`~lsst.daf.butler.CollectionType.RUN` collection in the central repo, removing it from the workspace (note that file artifacts remain where they are unless this is an external workspace), allowing the :attr:`~lsst.daf.butler.CollectionType.RUN` collection and its workspace to coexist for a while.

Sharded workspaces may be also useful for data release production campaign management.
Our current campaign management tooling prefers to do its own sharding, resulting in different :attr:`~lsst.daf.butler.CollectionType.RUN` collection for each shard (where a shard is typically hundreds or thousands of data IDs), and this model will continue to be viable and probably preferred due to the simplicity of one-to-one correspondence between :attr:`~lsst.daf.butler.CollectionType.RUN` collections (and in the future, workspaces) and batch submissions.
Sharding within a workspace just gives us another option, and declaring sharding dimensions with the current campaign management sharding approach still has the advantage of restricting tasks in exactly the way we'd like to guard against bad pipeline step definitions and operator error.

An unsharded workspace logically has empty sharding dimensions, and one shard identified by the empty data ID.
It permits all tasks since the set of empty dimensions is a subset of all dimension sets.

Data repository consistency
---------------------------

A serious but often ignored problem with QBB (and its predecessor, "execution butler") is that it is inconsistent with the nominal butler consistency model, in that it allows file artifacts to exist in a datastore root without having any record of those artifacts in the corresponding registry.
This consistency model is only "nominal" because it was never actually sound (deletions in particular have always been a problem), and hence we've considered it an acceptable casualty when working to limiting database access during execution (i.e. DMTN-177 :cite:`DMTN-177`).
DMTN-249 :cite:`DMTN-249` attempts to address this issue by defining a new consistency model that explicitly permits (and actually requires) file artifacts to exist prior to their datasets' inclusion in the registry, as long as those datasets are tracked as *possibly* existing in some other way that allows them to be found by any client of the central data repository (not merely, e.g. one that knows the user-provided location of an external quantum graph file, as is the case today).
The workspace concept described here is a realization of this new consistency model: the central data repository will have a record of all active (internal) workspaces (probably in a registry database table) and its full butler clients will have the ability to construct a :class:`WorkspaceButler` for these by name.
Workspace implementations are required to support the :meth:`WorkspaceButler.abandon` operation, which effectively requires that it have a way to find all of its file artifacts.

Concurrency and workspace consistency
-------------------------------------

Different workspaces belonging to the same central data repository are fully independent, and may be operated on concurrently in different processes with no danger of corruption.
This includes commits and workspace creation, and races for creation of the same workspace should result in at least one of the two attempts explicitly failing, and in no cases should hardware failures or concurrency yield an incorrectly-created workspace that can masquerade as a correct one.

Operations on workspaces should be idempotent either by being atomic or by being resumable from a partial state after interruption, with exceptions to this rule clearly documented.

*Most* operations on a workspaces are not expected to support or guard against concurrent operations on the same workspace, unless they are operating on disjoint sharding data IDs, in which case concurrency is required of all sharding workspace implementations.
In addition, some workspace-specific interfaces are expected to explicitly support concurrency under certain conditions; this is, after all, much of the reason we require workspaces to avoid central database access whenever possible.
Most prominent of these is the :meth:`PipelineWorkspaceButler.run_quanta` method, which guarantees correct behavior in the presence of concurrent calls as long as those calls identify disjoint sets of quanta to run, allowing it to be used as an execution primitive by higher-level logic capable of generating those disjoint sets (e.g. BPS).

In fact, we will define :meth:`PipelineWorkspaceButler.run_quanta` to *block* when it is given a quantum whose inputs are produced by other quanta that have not been run.
This is possible because this workspace implementation will also record quantum status (e.g. in per-quantum report files) for provenance, and we can modify the single-quantum execution logic to wait (e.g. for these files) to appear upstream.
This actually addresses a possible soundness problem in our current execution model: :class:`~lsst.resources.ResourcePath` backends other than POSIX and S3 may not guarantee that an output file artifacts write will propagate to all servers by the time a read or existence check is performed on a downstream node, let alone by the time the write call returns, and we think we've seen this problem in some WebDAV storage in the past.
With our current approach to execution this can result in silently incorrect execution, because the dataset will be assumed to have been not produced and may be ignored by the downstream.

Phasing out prediction-mode datastore
-------------------------------------

Allowing file datastore to operate in "prediction mode" (where a datastore client assumes datasets were written with the same formatters and file templates it is configured to use) was a necessary part of getting QBB working.
With workspaces, we hope to retire it, by saving datastore records along with the quantum status information the workspace will already be required to store, and which downstream quanta will be required to read.
This will allow us to simplify the datastore code and eliminate existence checks both during execution and when committing a quantum-graph-based workflow back to the central data repository.

Pipeline workspace actions
--------------------------

Creating a new pipeline workspace will not require that the quantum graph be produced immediately, though this will be a convenience operation (combining all of the steps described below) we expect to be frequently exercised.
Instead, only the pipeline must be provided up-front, and even that can be overwritten in development mode.
Tasks within that pipeline are activated next, which writes their init output file artifacts to the workspace (note that this now happens before a quantum graph is built).
Quanta can then be built, shard-by-shard; the workspace will remember which shards have quanta already.
Even executed quanta are not immutable until the workspace is committed - :class:`PipelineWorkspaceButler` will provide methods for quantum state transitions, like resetting a failed quantum so it can be attempted again.

Development mode will provide much more flexibility in the order these steps these can be performed, generally by resetting downstream steps and allowing output dataset to be clobbered.

.. _python-components-and-interfaces:

Python components and operation details
=======================================

Workspace construction and completion (``daf_butler``)
------------------------------------------------------

Workspaces are expected to play a central role in the DMTN-249 :cite:`DMTN-249` consistency model, in which file artifacts are written prior to the insertion of their metadata into the database, but only after some record is made in the central repository that those artifacts *may* exist.
This means workspace construction and removal operations need to be very careful about consistency in the presence of errors and concurrency.
This is complicated by the fact that workspaces are extensible; all concrete implementations will live downstream of ``daf_butler``.

We envision workspace creation to be aided by two helper classes that are typically defined along with a new :class:`WorkspaceButler` implementation:

- A :class:`WorkspaceFactory` is a callable that creates a new workspace from a full butler and some standard user-provided arguments, returning a :class:`WorkspaceButler` instance and a :class:`WorkspaceConfig` instance.
- :class:`WorkspaceConfig` is a pydantic model that records all state needed to make a new :class:`WorkspaceButler` client to an existing workspace, with a method to create a :class:`WorkspaceButler` from that state.

After a factory is used to write the rest of the workspace's initial state, the configuration is written to a JSON file in a predefined (by a URI template in the central repository butler configuration) location.
This file is read and used to make a new :class:`WorkspaceButler` instance without requiring access to the full butler.

This is all driven by a :meth:`~Butler.make_workspace` method on the full :class:`~lsst.daf.butler.Butler` class, for which the prototyping here includes a nominal implementation with detailed comments about how responsibility (especially for error-handling) is shared by different methods.

After an internal workspace has been created, a client :class:`WorkspaceButler` can be obtained from a full :class:`Butler` to the central repository by calling :meth:`Butler.get_workspace`, or without ever making a full butler by calling :meth:`ButlerConfig.make_workspace_butler`.
External workspace construction goes through :meth:`WorkspaceButler.make_external`.

We expect most concrete workspace implementations to define a ``butler`` subcommand for their creation, and for most end users to interact only with that command-line interface.

Committing a workspace goes through :meth:`WorkspaceButler.commit`, and abandoning one goes through :meth:`WorkspaceButler.abandon`.
Creating a new standalone repository goes through :meth:`WorkspaceButler.export`.
All of these have nominal implementations in the prototype, showing that they delegate to many of the same abstract methods to transfer their content to full butlers and remove anything that remains from the workspace.

To make exporting and external workspaces more useful, :meth:`WorkspaceButler.transfer_inputs` may be used to transfer the file artifacts of input datasets used by the workspace into the workspace itself.
This will allow the external workspace (depending on its implementation) to be used after fully severing its connection to the central data repository (e.g. copying it to a laptop).
These datasets are also included in the standalone data repository created by :meth:`WorkspaceButler.export`.

.. _quantum-graph-and-provenance-queries:

Quantum graph and provenance queries (``daf_butler``)
-----------------------------------------------------

This technote includes a simplified (relative to :cite:`DMTN-205`) proposal for provenance querying on the full butler, primarily because :class:`PipelineWorkspaceButler` will require major changes to the :class:`QuantumGraph` class, and it makes sense to include changes directed at using the same class (or at least a common base class) to report provenance to avoid yet another round of disruption.

The entry point is :meth:`Butler.query_provenance`, which delegates much of its functionality to a new parsed string-expression language, represented here by the :class:`QuantumGraphExpression` placeholder class.
I am quite optimistic that this will actually be pretty easy to implement, with one important caveat: we do not promise to efficiently resolve these expressions against large (let alone unbounded) sequences of collections, allowing us to implement expression resolution by something as simple as reading per-collection quantum-graph files into memory and calling `networkx <https://networkx.org/documentation/stable/index.html>`__ methods (which are generally O(N) in the size of the graph we've loaded).
A moderately complex expression could look something like this::

   isr..(..warp@{tract=9813, patch=22, visit=1228} | ..warp@{tract=9813, patch=22, visit=1230})

which evaluates to "all quanta and datasets downstream of the ``isr`` task that are also upstream of either of two particular ``warp`` datasets".
The standard set operators have their usual meanings, and ``..`` is used (just as in the butler data ID query language) to specify ranges.
In the context of a DAG, half-open ranges mean :func:`ancestors <networkx.algorithms.dag.ancestors>` or :func:`descendants <networkx.algorithms.dag.descendants>`, while::

   a..b

is a shortcut for::

   a.. & ..b

The return type is the new :class:`QuantumGraph`, which has very little in common with its :class:`lsst.pipe.base.QuantumGraph` predecessor in terms of its Python API, though of course they are conceptually very similar.
They may have more in common under the hood, especially with regards to serialization.
Major differences include:

- It does not attempt to hold task or config information, which makes it easier to put the class in ``daf_butler`` (where it needs to be for :meth:`Butler.query_provenance`).
  Instead it just has string task labels that can be looked up in a :class:`~lsst.pipe.base.Pipeline` or :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph`, which are stored as regular butler datasets in the same :attr:`~lsst.daf.butler.CollectionType.RUN` collection as the quanta they correspond to.

- Its interface is designed to allow implemenations to load the the :class:`~lsst.daf.butler.DataCoordinate` and :class:`~lsst.daf.butler.DatasetRef` information associated with a node only on demand.
  Our profiling has shown that saving and loading that information constitutes the vast majority of the time it takes to serialize and deserialize graphs, and we want to give future implementations the freedom to simply not do a lot of that work unless it's actually needed.
  We expect many provenance use cases to involve traversing a large graph while only requiring details about a small subset of the nodes.

- It records status for each node via the new :class:`QuantumStatus` and :class:`DatasetStatus` enums.
  These are not intended to provide human-readable error reports - this is a known gap in this proposal I'd like to resolve later - but they do provide a sufficiently rich set of states to represent human-driven *responses* to errors during processing as state transitions (e.g. :meth:`PipelineWorkspaceButler.accept_failed_quanta`, :meth:`~PipelineWorkspaceButler.poison_successful_quanta`, and :meth:`~PipelineWorkspaceButler.reset_quanta`), as well as a way to record those responses in provenance.

- Many of the current :class:`lsst.pipe.base.QuantumGraph` accessors are missing, having been replaced by a *bipartite* :class:`networkx.MultiDiGraph` accessor that includes nodes for datasets as well as quanta.
  Some of these may return as convenience methods for particularly common operations, but I like the idea of embracing the `networkx <https://networkx.org/documentation/stable/index.html>`__ graphs as the primary interface, having grown quite fond of them while working on :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph`.

- In order to be suitable for provenance query results that can span collections, a particular :class:`QuantumGraph` instance is not longer restricted to a single :attr:`~lsst.daf.butler.CollectionType.RUN` collection

The prototype here defines :class:`QuantumGraph` as an :class:`~abc.ABC`, which may or may not be ultimately necessary.
It may be that a single implementation could satisfy all quantum-oriented concrete workspaces as well as :meth:`Butler.query_provenance`.

How to handle the *storage* of committed quantum graphs is a question this technote does not attempt to fully answer.
The best answer probably involves a new registry "manager" interface and implementation, despite this implying the introduction of file-based storage to the registry (for the implementation we imagine doing first; a database-graph-storage option as in :cite:`DMTN-205` should be possible as well, if we find we need it).
We want changing the quantum graph storage format to be tracked as a repository migration, and using the registry manager paradigm is our established way of doing that.
Storing quantum graph content in database blob columns may be another good first-implementation option; this still saves us from having to fully map the quantum graph data structure to tables, while still allowing us to pull particularly useful/stable quantities into separate columns.

Pipeline workspace (``pipe_base``)
----------------------------------

The :class:`PipelineWorkspaceButler` that defines the new interface for low-level task execution has been prototyped here as an :class:`~abc.ABC` living in ``pipe_base``.
We could implement this ABC fully in ``ctrl_mpexec`` using modified versions of tools (e.g. :class:`~lsst.ctrl.mpexec.SingleQuantumExecutor`) already defined there, but it may also make sense to move most or all of the implementation to ``pipe_base``, perhaps leaving only :mod:`multiprocessing` execution to ``ctrl_mpexec`` while implementing serial, in-process execution in ``pipe_base``.
Even if the base class ends up concrete and capable of simple multiprocessing, I do expect it to support subclassing for specialized pipeline execution contexts (BPS, prompt processing, mocking), though composition or even command-line delegation may be a better option for some of these.

A pipeline workspace is initialized with a :class:`~lsst.pipe.base.Pipeline` or :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph`, and upon creation it stores these and (usually) the software versions currently in use as datasets (with empty data IDs) to the workspace.
Getter methods (:meth:`~PipelineWorkspaceButler.get_pipeline`, :meth:`~PipelineWorkspaceButler.get_pipeline_graph`, :meth:`~PipelineWorkspaceButler.get_packages`) provide access to the corresponding in-memory objects (these are not properties because they may do I/O).

A pipeline workspace can be initialized in or converted (irreversibly) to :attr:`~PipelineWorkspaceButler.development_mode`, which disables the saving of pipeline versions.
The pipeline associated with the workspace may be :meth:`reset <PipelineWorkspaceButler.reset_pipeline>` only in development mode.
Committing a development-mode pipeline workspace does not save provenance or configs to the central repository, because these are in general unreliable.
Often development-mode workspaces will ultimately be abandoned instead of committed.

After initialization, pipeline workspace operations proceed in roughly three stages:

1. Tasks are *activated*, which writes their init-outputs (including configs) to the workspace and marks them for inclusion (by default) in the next step.
   :meth:`~PipelineWorkspaceButler.activate_tasks` may be called multiple times as long as the order of the calls is consistent with the pipeline graph's topologicial ordering.
   Init-input and init-output dataset references are accessible via :meth:`~PipelineWorkspaceButler.get_init_input_refs` and :meth:`~PipelineWorkspaceButler.get_init_output_refs`.
   It is a major change here that these are written before a quantum graph is generated.
   This has always made more sense, since init datasets do not depend on data IDs or other the criteria that go into quantum graph generation, but we defer init-output writes today to after quantum graph generation in order to defer the first central-repository write operations as along as possible (particularly until after we are past the possibility of errors in quantum graph generation).
   With workspaces deferring committing any datasets to the central repository already, this is no longer a concern and we are free to move init-output writes earlier.

2. Quanta are built for the active tasks or a subset thereof via one or more calls to :meth:`~PipelineWorkspaceButler.build_quanta`.
   Quanta are persisted to the workspace in an implementation-defined way (the first implementation will use files similar to the ``.qgraph`` files we currently save externally).
   In a sharded workspace, the quanta for different disjoint sets of shard data IDs (not arbitrary data IDs!) may be built separately - at first incrementally, by serial calls to :meth:`~PipelineWorkspaceButler.build_quanta`, but eventually concurrent calls or :mod:`multiprocessing` parallelism should be possible as well.
   We also eventually expect to support building the quanta for subsequent tasks in separate (but serial) calls, allowing different ``where`` expressions (etc) for different tasks within the same graph.
   This is actually a really big deal: it's a major step towards finally addressing the infamous :jira:`DM-21904` problem, which is what prevents us from building a correct quantum graph for a full DRP pipeline on even small amounts of data.
   Very large scale productions (of the sort handled by Campaign Management) will continue to be split up into multiple workspaces/collections with external sharding, but smaller CI and developer-initiated runs of the full pipeline should possible within a single workspace, with a single batch submission and a single output :attr:`~lsst.daf.butler.CollectionType.RUN` collection.
   :meth:`WorkspaceButler.transfer_inputs` does nothing on a :class:`PipelineWorkspaceButler` unless quanta have already been generated.

3. Quanta are executed via one or more calls to :meth:`~PipelineWorkspaceButler.run_quanta`.
   Here the base class does specify correct behavior in the presence of concurrent calls, *if* the quanta matching the arguments to those calls are disjoint: implementations must not attempt to execute a matching quantum until its upstream quanta have executed successfully, and block until this is the case.
   Quanta that have already been run are automatically skipped, while those that fail continue to block everything downstream.
   :meth:`~PipelineWorkspaceButler.run_quanta` accepts both simple UUIDs and rich expressions in its specification of which quanta to run, but only the former is guaranteed to be O(1) in the size of the graph; anything else could traversing the full graph, which is O(N).
   Passing quantum UUIDs to :class:`PipelineWorkspaceButler` is what we expect BPS to do under the hood, so this has to be fast, while everything
   else is a convenience.

:class:`PipelineWorkspaceButler` has several methods for tracking these state transitions:

- :attr:`~PipelineWorkspaceButler.active_tasks`: the set of currently active tasks;

- :meth:`~PipelineWorkspaceButler.get_built_quanta_summary`: the set of task and shard data ID combinations for which quanta have already been built;

- :meth:`~PipelineWorkspaceButler.query_quanta`: the execution status of all built quanta.
  This method's signature is identical to that of :meth:`~Butler.query_provenance`, but it is limited to the workspace's own quanta.
  Since it reports on quanta that could be executing concurrently, the base class makes very weak guarantees about how up-to-date its information may be.
  It may also be far less efficient than WMS-based approaches to getting status, especially for queries where the entire quantum graph has to be traversed.
  The ideal scenario (but probably not a near-term one) for BPS would be a WMS-specific workspace provided by a BPS plugin implementing this method to use WMS approaches first, delegating to :class:`PipelineWorkspaceButler` via composition or inheritance with narrower queries only when necessary.

In development mode, task init-outputs (including configs) are rewritten on every call to :meth:`~PipelineWorkspaceButler.build_quanta` or :meth:`~PipelineWorkspaceButler.run_quanta`, because we have no way to check whether their contents would change when software versions and configs are not controlled.
Quanta are not automatically rebuilt, because graph building is often slow, most development changes to tasks do not change graph structure, and it is reasonable to expect developers to be aware of when it does.
We *can* efficiently most detect changes to the :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph` due to code or config changes and warn or fail when the quantum graph is not rebuilt when it should be, but this is not rigorously true: changes to a :meth:`~lsst.pipe.base.PipelineTaskConnections.adjustQuantum` implementation are invisible unless the quantum graph is actually rebuilt.
Rebuilding the quantum graph for a shard-task combination that already has quanta is permitted only in development mode, where it immediately deletes all outputs that may exist from executing the previous quanta for that combination.

:class:`PipelineWorkspaceButler` also provides methods for changing the status of already-executed quanta, even outside development mode, with consistent changes to (including, sometimes, removal of) their output datasets:

- :meth:`~PipelineWorkspaceButler.accept_failed_quanta` marks failed quanta as successful (while remembering their original failed state and error conditions), allowing downstream quanta to be run as long as they can do so without the outputs the failed quanta would have produced.
  Invoking this on all failed quanta is broadly equivalent to committing all successfully-produced datasets to the central data repository and starting a new workspace with the committed :attr:`~lsst.daf.butler.CollectionType.RUN` collection as input, which is effectively how failures are accepted today.
  It is worth considering whether we should require all failed quanta to be accepted before a workspace is committed (outside development mode) to make this explicit rather than implicit.
- :meth:`~PipelineWorkspaceButler.poison_successful_quanta` marks successful quanta as failures, as might need to occur if QA on their outputs revealed a serious problem that did not result in an exception.
  Downstream quanta that had already executed are also marked as failed - we assume bad inputs implies bad outputs.
  This does not immediately remove output datasets, but it does mark them as :class:`INVALIDATED <DatasetStatus>`, preventing them from being committed unless they are again explicitly reclassified.
- :meth:`~PipelineWorkspaceButler.reset_quanta` resets all matching quanta to the just-built :class:`PREDICTED <QuantumStatus>` state, deleting any existing outputs.

The :meth:`~PipelineWorkspaceButler.activate_tasks` and :meth:`~PipelineWorkspaceButler.build_quanta` methods accept  a parsed string :class:`PipelineGraphExpression` similar to (but simpler than) the :class:`QuantumGraphExpression` described in :ref:`quantum-graph-and-provenance-queries`, which just uses dataset types and task labels as identifiers since data IDs and UUIDs are irrelevant.
We also expect to enable this expression language to be used in the definition of labeled subsets in pipeline YAML files in the future.

.. _cli:

Command-line interface
======================

This technote does not yet include a prototype command-line interface for :class:`PipelineWorkspaceButler`, despite this being the main way we expect most users to interact with it.
We do expect the CLI to involve a suite of new ``butler`` subcommands (possibly another level of nesting, i.e. ``butler workspace build-quanta``, ``butler workspace run-quanta``), and for the current ``pipetask`` tool to be deprecated in full rather than migrated.
Details will be added to this technote on a future ticket.

.. _implementation-notes:

Implementation notes
====================

While the interface of :class:`PipelineWorkspaceButler` is intended to permit implementations that store their persistent state in other ways, such as a NoSQL database (Redis seems particularly well suited), the initial implementation will use files.
We'll need a file-based implemenatation in the long term anyway to make it easy to set up a minimal middleware environment without the sort administrative responsibilities nearly all databases involve.

These files will *sometimes* be managed by a :class:`~lsst.daf.butler.Datastore`; certainly this will be the case for the file artifacts of datasets that could be eventually committed as-is back to the central repository, including the workspace-written datasets like ``packages`` and the new ``pipeline`` and ``pipeline_graph`` datasets.

Quantum graphs don't fit as well into :class:`~lsst.daf.butler.Datastore` management.
This is partly a conceptual issue - we expect quantum graph files to continue to provide the sort of dataset metadata (data IDs, datastore records) the database provides for the central repository (as in today's QBB), so putting quantum graph files into a :class:`~lsst.daf.butler.Datastore` is a little like putting a SQLite database file into a :class:`~lsst.daf.butler.Datastore`.
And this isn't entirely conceptual - like SQLite database files, we may want to be able to modify quantum graph files in-place (even if that's just to append), and that's not a door we want to open for the butler dataset concept.
Quantum graph files also exist on the opposite end of the spectrum from regular pipeline output datasets in terms of whether we will want to rewrite them rather than just ingest them on commit.
Even if provenance quantum graphs in the central repository are stored in files initially (the plan in mind here), we probably want to strip out all of the dimension record data they hold that is wholly redundant with what's in the database, and since concerns about modifying the graphs disappear after commit, we probably want to consolidate the graph information into fewer files.

This last point also holds for a new category of file-based state we'll need to add for :class:`PipelineWorkspaceButler`: per-quantum status files that are written after each a quantum is executed.
In addition to status flags and exception information for failures, these are can hold output-dataset datastore records, and their existence is what downstream quanta will block on in :class:`PipelineWorkspaceButler.run_quanta`.
These will be scanned as-is by :class:`PipelineWorkspaceButler.query_quanta`, since that's all it can do, but we really want to merge this status information with the rest of the quantum graph on commit (while dropping the redundant dimension records, and moving datastore records into the central database), potentially making :class:`Butler.query_provenance` much more efficient in terms of storage and access cost.

.. _prototype-code:

Prototype code
==============

This technote's git repository includes two Python files with stubs representing new interfaces for the ``daf_butler`` and ``pipe_base`` packages.
These can be inspected directly, but the most important parts are included here so classes and methods can be referenced by the preceding sections.
This occasionally includes proto-implementations, but only when these are particularly illustrative of the relationships between interfaces.

daf_butler
----------

.. py:class:: Butler

   .. py:method:: make_workspace

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: Butler.make_workspace

   .. py:method:: get_workspace

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: Butler.make_workspace

   .. py:method:: query_provenance

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: Butler.query_provenance

.. py:class:: ButlerConfig

   .. py:method:: make_workspace_butler

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: ButlerConfig.make_workspace_butler

.. py:class:: WorkspaceFactory

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: WorkspaceFactory

.. py:class:: WorkspaceConfig

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: WorkspaceConfig

.. py:class:: WorkspaceButler

   .. py:method:: make_external

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: WorkspaceButler.make_external

   .. py:method:: abandon

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: WorkspaceButler.abandon

   .. py:method:: commit

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: WorkspaceButler.commit

   .. py:method:: export

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: WorkspaceButler.commit

   .. py:method:: transfer_inputs

      .. literalinclude:: daf_butler.py
         :language: py
         :pyobject: WorkspaceButler.transfer_inputs

.. py:class:: QuantumGraph

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: QuantumGraph

.. py:class:: QuantumGraphExpression

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: QuantumGraphExpression

.. py:class:: QuantumStatus

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: QuantumStatus

.. py:class:: DatasetStatus

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: DatasetStatus


pipe_base
---------

.. py:class:: PipelineWorkspaceButler

   .. py:attribute:: development_mode

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.development_mode

   .. py:method:: get_pipeline_graph

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_pipeline_graph

   .. py:method:: get_pipeline

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_pipeline

   .. py:method:: reset_pipeline

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.reset_pipeline

   .. py:method:: get_packages

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_packages

   .. py:attribute:: active_tasks

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.active_tasks

   .. py:method:: activate_tasks

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.activate_tasks

   .. py:method:: get_init_input_refs

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_init_input_refs

   .. py:method:: get_init_output_refs

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_init_output_refs

   .. py:method:: build_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.build_quanta

   .. py:method:: get_built_quanta_summary

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.get_built_quanta_summary

   .. py:method:: query_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.query_quanta

   .. py:method:: run_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.run_quanta

   .. py:method:: accept_failed_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.accept_failed_quanta

   .. py:method:: poison_successful_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.poison_successful_quanta

   .. py:method:: reset_quanta

      .. literalinclude:: pipe_base.py
         :language: py
         :pyobject: PipelineWorkspaceButler.reset_quanta

.. py:class:: PipelineGraphExpression

   .. literalinclude:: pipe_base.py
      :language: py
      :pyobject: PipelineGraphExpression


References
==========

.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
