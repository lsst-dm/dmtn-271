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
Our main focus here will be a :class:`PipelineWorkspaceButler` subclass that serves as a ``pipetask`` replacement, and we imagine it storing its persistent state (mostly quantum graphs) using files.
Whether those files are managed by a prediction-mode datastore or something else (e.g. direct use of :class:`~lsst.resources.ResourcePath` and probably some datastore support tooling, like the file template system) is an implementation detail
Implementations with similar capabilities that use scalable NoSQL databases for persistent state should also be possible (but it is not obvious that this will be necessary).
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

We have one major use case that does not fit as well with this model, at leas as described so far: prompt processing runs one ``visit`` or one ``{visit, detector}`` at a time, appending these results to a single :attr:`~lsst.daf.butler.CollectionType.RUN` collection that typically covers an entire night's processing.
The switch from ``pipetask`` to workspaces wouldn't actually *require* any major changes to the prompt processing framework's architecture, because each prompt processing worker has its own full SQL-backed butler, with the fan-out to workers and long-term persistence represented as transfers between data repositories.
Workspaces would be created and committed wholly within individual workers,  requiring some minor API changes to low-level tooling like :class:`~lsst.ctrl.mpexec.SeparablePipelineExecutor`, but no more.

An alternative approach in which workspaces are more tightly integrated with prompt processing is both intriguing and more disruptive.
In this model, prompt processing would have a custom :class:`WorkspaceButler` implementation that represents a client of a central-butler workspace existing on a worker node.
This would give prompt-processing complete control over quantum graph generation and how it is interleaved with execution, allowing the graph to be built incrementally as new information (first the ``nextVisit`` event, then a ``raw`` file artifact, and then perhaps a ``raw`` for a second stamp), with each processing step starting as soon as possible.
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
The workspace concept described here is a realization of this new consistency model: the central data repository will have a record of all active (internal) workspaces (probably in a registry database table) and its full butler clients will have the ability to construct a :class:`WorkspaceButler` for these by name, and a workspace is required to support the :meth:`WorkspaceButler.abandon` operation, which effectively requires that it have a way to find all of its file artifacts.

Concurrency and workspace consistency
-------------------------------------

Different workspaces belonging to the same central data repository are fully independent, and may be operated on concurrently in different processes with no danger of corruption.
This includes commits and workspace creation, and races for creation of the same workspace should result in at least one of the two attempts explicitly failing, and no danger of an incorrect workspace being silently created.

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

.. _new-interfaces:

New interfaces
==============

Workspace construction and completion (``daf_butler``)
------------------------------------------------------

Workspaces are expected to play a central role in the DMTN-249 :cite:`DMTN-248` consistency model, in which file artifacts are written prior to the insertion of their metadata into the database, but only after some record is made in the central repository that those artifacts *may* exist.
This means workspace construction and removal operations need to be very careful about consistency in the presence of errors and concurrency.
This is complicated by the fact that workspaces are extensible; all concrete implementations will live downstream of `lsst.daf.butler`.

We envision workspace creation to be aided by two helper classes that are typically defined along with a new :class:`WorkspaceButler` implementation:

- A :class:`WorkspaceFactory` is a callable that creates a new workspace from a full butler and some standard user-provided arguments, returning a :class:`WorkspaceButler` instance and a :class:`WorkspaceConfig` instance.
- :class:`WorkspaceConfig` is a pydantic model that records all state needed to make a new :class:`WorkspaceButler` client to an existing workspace, with a method to create a :class:`WorkspaceButler` from that state.

After a factory is used to write rest of the workspace's initial state, the configuration is written to a JSON file in a predefined (by a URI template in the central repository butler configuration) location.
This file is read and used to make a new :class:`WorkspaceButler` instance without requiring access to the full butler.

This is all driven by a :meth:`~Butler.make_workspace` method on the full :class:`~lsst.daf.butler.Butler` class, for which the prototyping here includes a nominal implementation with detailed comments about how responsibility (especially for error-handling) is shared by different methods.

After an internal workspace has been created, a client :class:`WorkspaceButler` can be obtained from a full :class:`Butler` to the central repository by calling :meth:`Butler.get_workspace`, or without ever making a full butler by calling :meth:`ButlerConfig.make_workspace_butler`.
External workspace construction goes through :meth:`WorkspaceButler.make_external`.

We expect most concrete workspace implementations to define a ``butler`` subcommand for their creation, and for most end users to interact only with that command-line interface.

Committing a workspace goes through :meth:`WorkspaceButler.commit`, and abandoning one goes through :meth:`WorkspaceButler.abandon`.
Creating a new standalone repository goes through :meth:`WorkspaceButler.export`.
All of these have nominal implementations in the prototype, showing that they delegate to many of the same abstract methods to transfer their content to full butlers and remove anything that remains from the workspace.

Quantum graph and provenance queries (``daf_butler``)
-----------------------------------------------------

This technote includes a simplified (relative to :cite:`DMTN-205`) proposal for provenance querying on the full butler, primarily because :class:`PipelineWorkspaceButler` will require major changes to the :class:`QuantumGraph` class, and it makes sense to include changes directed at using the same class (or at least a common base class) to report provenance to avoid yet another round of disruption.

The entry point is :meth:`Butler.query_provenance`, which delegates much of its functionality to a new parsed string-expression language, represented here by the :class:`QuantumGraphExpression` placeholder class.
I am quite optimistic that this will actually be pretty easy to implement, with one important caveat: we do not promise to efficiently resolve these expressions against large (let alone unbounded) sequences of collections, allowing us to implement expression resolution by something as simple as reading per-collection quantum-graph files into memory and calling `networkx <https://networkx.org/documentation/stable/index.html>`__ methods.
A moderately complex expression could look something like this::

   isr..(..warp@{tract=9813, patch=22, visit=1228} | ..warp@{tract=9813, patch=22, visit=1230})

which evaluates to "all quanta and datasets downstream of the ``isr`` task that are also upstream of either of two particular ``warp`` datasets".
The standard set operators have their usual meanings, and ``..`` is used (just as in the butler data ID query language) to specify ranges.
In the context of a DAG, half-open ranges mean :py:func:`ancestors <networkx.algorithms.dag.ancestors>` or :py:func:`descendants <networkx.algorithms.dag.descendants>`, while::

   a..b

is a shortcut for::

   a.. & ..b

The return type is the new :class:`QuantumGraph`, which has very little in common with its :class:`lsst.pipe.base.QuantumGraph` predecessor in terms of its Python API, though of course they are conceptually very similar.
They may have more in common under the hood, especially with regards to serialization.
Major differences include:

- It does not attempt to hold task or config information, which makes it easier to put the class in ``daf_butler`` (where it needs to be for :meth:`Butler.query_provenance`).
  Instead it just has string task labels that can be looked up in a :class:`~lsst.pipe.base.Pipeline` or :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph`, which are stored as regular butler datasets in the same :attr:`~lsst.daf.butler.CollectionType.RUN` collection as the quanta they correspond to.

- Its interface is designed to allow implemenations to load the the :class:`~lsst.daf.butler.DataCoordinate` and :class:`~lsst.daf.butler.DatasetRef` information associated with a node only on demand.
  Our profiling has shown that saving and loading that information constitutes the vast majority of the time it takes to serialize and deserialize graphs, and we want to give future implementations to simply not do a lot of that work unless it's actually needed.
  We expect a lot of provenance use cases to involve traversing a large graph but only requiring details about a small subset of the nodes.

- It records status for each node via the new :class:`QuantumStatus` and :class:`DatasetStatus` enums.
  These are not intended to provide human-readable error reports - this is a known gap in this proposal I'd like to resolve later - but they do provide a sufficiently rich set of states to represent human-driven *responses* to errors during processing as state transitions (e.g. :meth:`PipelineWorkspaceButler.accept_failed_quanta`, :meth:`~PipelineWorkspaceButler.poison_successful_quanta`, and :meth:`~PipelineWorkspaceButler.reset_quanta`), as well as a way to record those responses in provenance.

- Many of the current :class:`lsst.pipe.base.QuantumGraph` accessors are missing, having been replaced by a *bipartite* :class:`networkx.MultiDiGraph` accessor that includes nodes for datasets as well as quanta.
  Some of these may return as convenience methods for particularly common operations, but I like the idea of embracing the `networkx <https://networkx.org/documentation/stable/index.html>`__ graphs as the primary interface, having grown quite fond of them while working on :class:`~lsst.pipe.base.pipeline_graph.PipelineGraph`.

- In order to be suitable for provenance query results that can span collections, a particular :class:`QuantumGraph` instance is not longer restricted to a single :attr:`~lsst.daf.butler.CollectionType.RUN` collection

The prototype here defines :class:`QuantumGraph` as an ABC, which may or may not be ultimately necessary.
It may be that a single implementation could satisfy all quantum-oriented concrete workspaces as well as :meth:`Butler.query_provenance`.

Pipeline workspace (``pipe_base``)
----------------------------------

.. _prototype-code:

Prototype code
==============

This technote's git repository includes two Python files with stubs representing new interfaces for the ``daf_butler`` and ``pipe_base`` packages.
These can be inspected directly, but the most important parts are included here so classes and methods can be referenced by the preceding sections.
This occasionally includes proto-implementations, but only when these are particularly illustrative of the relationships between interfaces.

daf_butler
----------

.. py:class:: WorkspaceFactory

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: WorkspaceFactory

.. py:class:: WorkspaceConfig

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: WorkspaceConfig

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

.. py:class:: QuantumGraphExpression

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: QuantumGraphExpression

.. py:class:: QuantumGraph

   .. literalinclude:: daf_butler.py
      :language: py
      :pyobject: QuantumGraph

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



References
==========

.. rubric:: References

.. bibliography:: local.bib lsstbib/books.bib lsstbib/lsst.bib lsstbib/lsst-dm.bib lsstbib/refs.bib lsstbib/refs_ads.bib
   :style: lsst_aa
