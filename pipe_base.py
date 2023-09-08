from __future__ import annotations

from abc import abstractmethod
import uuid

from types import EllipsisType
from typing import Any, TypeAlias
from collections.abc import Iterable, Mapping, Set
from lsst.resources import ResourcePathExpression
from lsst.pipe.base import Pipeline, PipelineGraph
from lsst.utils.packages import Packages
from lsst.daf.butler import DataId, DataCoordinate, DatasetRef

from daf_butler import WorkspaceButler, QuantumGraph, QuantumGraphExpression


class PipelineGraphExpression:
    """Placeholder for a parsed expression that subsets a pipeline graph.

    This is expected to include:

    - the standard set-theory operations (intersection, union, difference,
      symmetric difference, inversion);
    - literal sets of task labels and dataset type names;
    - regular expressions or shell-style globs on task labels and dataset type
      names;
    - range syntax for ancestors and descendants of task labels and dataset
      type names (e.g. ``..b`` and ``a..``), possibly with a shortcut for
      an intersection of the two (e.g. ``a..b``);
    - bind aliases for task labels, dataset type names, and sets thereof, to
      be satisfied with an extra dictionary mapping bind alias to `str` or
      ``collections.abc.Set[str]``.
    """

PipelineGraphWildcard: TypeAlias = PipelineGraphExpression | str | Iterable[str] | EllipsisType


class PipelineWorkspaceButler(WorkspaceButler):
    """Workspace butler for basic pipeline execution.

    This workspace butler assumes tasks are `PipelineTasks` and is capable of
    running them in serial in the current process.

    A trivial subclass in ``ctrl_mpexec`` could extend this to running them in
    parallel with multiprocessing (yes, I'm proposing we gut ``ctrl_mpexec``
    and move most of its functionality to ``pipe_base``).

    I think it'd be cool if BPS could also use the `WorkspaceButler` interface,
    or even inherit from `PipelineWorkspaceButler`, but I don't know how
    feasible that actually is.  And we don't have to do that to make BPS work
    with the workspace-butler system; it could just change what the
    command-lines in BPS YAML files are.

    Sharded workspaces of this type can build graphs and run quanta on each
    ``(task, data ID)`` combination separately (as long as tasks are processed
    in topological order for each data ID), in addition to being able to commit
    each data ID independently.
    """

    @property
    @abstractmethod
    def development_mode(self) -> bool:
        """Whether this workspace is in development mode, in which version and
        configuration changes are permitted but provenance is limited.

        This can always be set to enter development mode (`True`), but this
        is irreversible.
        """
        raise NotImplementedError()

    @development_mode.setter
    @abstractmethod
    def development_mode(self, value: bool) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_pipeline_graph(self) -> PipelineGraph:
        """Return the pipeline graph associated with this workspace.

        This is always the complete pipeline; it in general includes tasks that
        have not been activated.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_pipeline(self) -> Pipeline | None:
        """Return the pipeline associated with this workspace.

        This is always the complete pipeline; it in general includes tasks that
        have not been activated.  There may not be any pipeline if only a
        pipeline graph was provided at workspace construction.
        """
        raise NotImplementedError()

    @abstractmethod
    def reset_pipeline(
        self,
        pipeline: ResourcePathExpression | Pipeline | PipelineGraph,
    ) -> None:
        """Update the pipeline associated with this workspace.

        This is only permitted if the workspace in development mode or if no
        tasks have been activated.

        This operation requires read access to the central data repository
        database (to resolve dataset types).
        """
        raise NotImplementedError()

    @abstractmethod
    def get_packages(self) -> Packages | None:
        """Return the software versions frozen with this workspace.

        This is `None` if and only if the workspace is in development mode.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def active_tasks(self) -> Set[str]:
        """The labels of the tasks considered active in this workspace.

        This is always a subset of the pipeline graph's task labels.  Active
        tasks have had their init-outputs (including configuration) written,
        and only active tasks may have quanta built.
        """
        raise NotImplementedError()

    @abstractmethod
    def activate_tasks(self, spec: PipelineGraphWildcard = ..., /, ) -> None:
        """Activate tasks matching the given pattern.

        This writes init-outputs for the given tasks.  Activating a task whose
        init-inputs are not available either in the input collections or the
        workspace itself is an error.

        Reactivating an already-active task in development mode causes
        init-outputs to be re-written.  Outside development mode it checks that
        software versions have not changed and hence init-outputs do not need
        to be written, and raises if software versions have changed.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_init_input_refs(self, task_label: str) -> Mapping[str, DatasetRef]:
        """Return the init-input dataset references for an activated task.

        Mapping keys are connection names.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_init_output_refs(self, task_label: str) -> Mapping[str, DatasetRef]:
        """Return the init-output dataset references for an activated task.

        Mapping keys are connection names.
        """
        raise NotImplementedError()

    @abstractmethod
    def build_quanta(
        self,
        *,
        tasks: PipelineGraphWildcard | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        shards: Iterable[DataCoordinate] | None = None,
        **kwargs: Any,
    ) -> None:
        """Build a quantum graph, extending any existing graph.

        Parameters
        ----------
        tasks
            Specification for the tasks to include.  If `None`, all active
            tasks are used.  For any other value, matching tasks that are not
            already active will be activated.
        where : `str`, optional
            Data ID query string.
        bind : `~collections.abc.Mapping`, optional
            Values to substitute for aliases in ``where``.
        data_id : `~lsst.daf.butler.DataCoordinate` or mapping, optional
            Data ID that constrains all quanta.
        shards : `~collections.abc.Iterable` [ \
                ~`lsst.daf.butler.DataCoordinate` ], optional
            Data IDs that identify the sharding dimensions; must be provided if
            the sharding dimensions for this workspace are not empty.
        **kwargs
            Additional data ID key-value pairs, overriding and extending
            ``data_id``.

        Notes
        -----
        This may be called multiple times with different tasks or shards, but
        only with tasks in topological order (for each shard data ID).  This
        allows a large quantum graph to be built incrementally, with different
        data ID constraints for different tasks.

        Rebuilding the quanta for a task-shard combination that has already
        been built does nothing unless the workspace is in development mode. In
        development mode, rebuilding quanta for a task-shard combination will
        delete all downstream quanta that have not be executed, and raise if
        any downstream quanta have already been executed (`reset_quanta` can be
        used to deal with already-executed quanta in advance). Rebuilt quanta
        and their output datasets are assigned new UUIDs.

        This operation requires read access and temporary-table write access to
        the central data repository database.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_built_quanta_summary(self) -> Set[tuple[str, DataCoordinate]]:
        """Report the combinations of task labels and sharding data IDs for
        which quantum graphs have already been built.

        Empty data IDs are returned for workspaces with no sharding dimensions.
        """
        raise NotImplementedError()

    @abstractmethod
    def query_quanta(
        self,
        *,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> QuantumGraph:
        """Query for quanta that have already been built and possibly
        executed.

        The returned QuantumGraph is a snapshot of the workspace's state, not a
        view.  If quanta are currently being executed when this is called, the
        status for different quanta may not reflect the same instant in time,
        but the states for a single quantum and its output datasets are always
        consistent (but possibly already out-of-date by the time the method
        returns).  It is an error to call this method while quanta are being
        built, but workspaces are not required to guard against this.
        """
        raise NotImplementedError()

    @abstractmethod
    def run_quanta(
        self,
        *,
        quanta: uuid.UUID | Iterable[uuid.UUID] | EllipsisType = ...,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> None:
        """Execute tasks on quanta.

        TODO
        """
        raise NotImplementedError()

    @abstractmethod
    def get_run_quanta_summary(self) -> Set[tuple[str, DataCoordinate]]:
        """Report the combinations of task labels and sharding data IDs for
        which quantum graphs have already been run.

        Empty data IDs are returned for workspaces with no sharding dimensions.
        """
        raise NotImplementedError()

    @abstractmethod
    def accept_failed_quanta(
        self,
        *,
        quanta: uuid.UUID | Iterable[uuid.UUID] | EllipsisType = ...,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> None:
        """Change the status of matching quanta that currently also have status
        `~QuantumStatus.FAILED` to `~QuantumStatus.SUCCESSFUL`.

        Existing outputs (which should already be marked as
        `~DatasetStatus.INVALIDATED`) will have their status set to
        `~DatasetStatus.PRESENT`.
        """
        raise NotImplementedError()

    @abstractmethod
    def poison_successful_quanta(
        self,
        *,
        quanta: uuid.UUID | Iterable[uuid.UUID] | EllipsisType = ...,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> None:
        """Change the status of matching quanta that currently also have status
        `~QuantumStatus.SUCCESSFUL` to `~QuantumStatus.FAILED`, set all
        downstream quanta to `~QuantumStatus.PREDICTED`
        """
        raise NotImplementedError()

    @abstractmethod
    def reset_quanta(
        self,
        *,
        quanta: uuid.UUID | Iterable[uuid.UUID] | EllipsisType = ...,
        where: QuantumGraphExpression = ...,
        bind: Mapping[str, Any] | None = None,
        data_id: DataId | None = None,
        **kwargs: Any,
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def transfer_inputs(self, transfer: str | None = "copy") -> None:
        """Transfer file artifacts for all overall-input datasets to the
        workspace root and update the URIs used to fetch them during execution
        accordingly.

        Notes
        -----
        This method can only be called if the workspace root is outside the
        central data repository root.  After it has been called it should be
        possible to relocate the workspace directory to a system disconnected
        from the central data repository and still call `run_quanta` or
        `export`.

        This requires quanta to already exist, so calling it before building
        the complete quantum graph is usually a mistake, but workspace
        implementations are not required to guard against this.  It may be
        called either before or after `run_quanta`.
        """
        raise NotImplementedError()
