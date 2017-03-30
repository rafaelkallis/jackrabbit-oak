/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.LoggingCompositeHook;
import org.apache.jackrabbit.oak.upgrade.checkpoint.CheckpointRetriever;
import org.apache.jackrabbit.oak.upgrade.cli.node.TarNodeStore;
import org.apache.jackrabbit.oak.upgrade.nodestate.NameFilteringNodeState;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.LoggingReporter;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.ReportingNodeState;
import org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil;
import org.apache.jackrabbit.oak.upgrade.version.VersionableEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.NT_REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_EXCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_INCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_MERGE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.calculateEffectiveIncludePaths;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.createIndexEditorProvider;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.createTypeEditorProvider;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.markIndexesToBeRebuilt;
import static org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier.copyProperties;
import static org.apache.jackrabbit.oak.upgrade.version.VersionCopier.copyVersionStorage;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionStorage;

public class RepositorySidegrade {

    private static final Logger LOG = LoggerFactory.getLogger(RepositorySidegrade.class);

    private static final int LOG_NODE_COPY = Integer.getInteger("oak.upgrade.logNodeCopy", 10000);

    private static final String WORKSPACE_NAME_PROP = "oak.upgrade.workspaceName";

    /**
     * Target node store.
     */
    private final NodeStore target;

    private final NodeStore source;

    /**
     * Paths to include during the copy process. Defaults to the root path "/".
     */
    private Set<String> includePaths = DEFAULT_INCLUDE_PATHS;

    /**
     * Paths to exclude during the copy process. Empty by default.
     */
    private Set<String> excludePaths = DEFAULT_EXCLUDE_PATHS;

    /**
     * Paths to merge during the copy process. Empty by default.
     */
    private Set<String> mergePaths = DEFAULT_MERGE_PATHS;

    private boolean skipCheckpoints = false;

    private boolean includeIndex = false;

    private boolean filterLongNames = true;

    private boolean verify = false;

    private boolean onlyVerify = false;

    private List<CommitHook> customCommitHooks = null;

    VersionCopyConfiguration versionCopyConfiguration = new VersionCopyConfiguration();

    /**
     * Configures the version storage copy. Be default all versions are copied.
     * One may disable it completely by setting {@code null} here or limit it to
     * a selected date range: {@code <minDate, now()>}.
     * 
     * @param minDate
     *            minimum date of the versions to copy or {@code null} to
     *            disable the storage version copying completely. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyVersions(minDate);
    }

    /**
     * Configures copying of the orphaned version histories (eg. ones that are
     * not referenced by the existing nodes). By default all orphaned version
     * histories are copied. One may disable it completely by setting
     * {@code null} here or limit it to a selected date range:
     * {@code <minDate, now()>}. <br>
     * <br>
     * Please notice, that this option is overriden by the
     * {@link #setCopyVersions(Calendar)}. You can't copy orphaned versions
     * older than set in {@link #setCopyVersions(Calendar)} and if you set
     * {@code null} there, this option will be ignored.
     * 
     * @param minDate
     *            minimum date of the orphaned versions to copy or {@code null}
     *            to not copy them at all. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyOrphanedVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyOrphanedVersions(minDate);
    }

    /**
     * Creates a tool for copying the full contents of the source repository
     * to the given target repository. Any existing content in the target
     * repository will be overwritten.
     *
     * @param source source node store
     * @param target target node store
     */
    public RepositorySidegrade(NodeStore source, NodeStore target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Returns the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @return the list of custom CommitHooks
     */
    public List<CommitHook> getCustomCommitHooks() {
        return customCommitHooks;
    }

    /**
     * Sets the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @param customCommitHooks the list of custom CommitHooks
     */
    public void setCustomCommitHooks(List<CommitHook> customCommitHooks) {
        this.customCommitHooks = customCommitHooks;
    }

    /**
     * Sets the paths that should be included when the source repository
     * is copied to the target repository.
     *
     * @param includes Paths to be included in the copy.
     */
    public void setIncludes(@Nonnull String... includes) {
        this.includePaths = copyOf(checkNotNull(includes));
    }

    /**
     * Sets the paths that should be excluded when the source repository
     * is copied to the target repository.
     *
     * @param excludes Paths to be excluded from the copy.
     */
    public void setExcludes(@Nonnull String... excludes) {
        this.excludePaths = copyOf(checkNotNull(excludes));
    }

    public void setIncludeIndex(boolean includeIndex) {
        this.includeIndex = includeIndex;
    }

    /**
     * Sets the paths that should be merged when the source repository
     * is copied to the target repository.
     *
     * @param merges Paths to be merged during copy.
     */
    public void setMerges(@Nonnull String... merges) {
        this.mergePaths = copyOf(checkNotNull(merges));
    }

    public void setFilterLongNames(boolean filterLongNames) {
        this.filterLongNames = filterLongNames;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    public void setOnlyVerify(boolean onlyVerify) {
        this.onlyVerify = onlyVerify;
    }

    public void setSkipCheckpoints(boolean skipCheckpoints) {
        this.skipCheckpoints = skipCheckpoints;
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy() throws RepositoryException {
        try {
            if (!onlyVerify) {
                if (VersionHistoryUtil.getVersionStorage(target.getRoot()).exists() && !versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                    LOG.warn("The version storage on destination already exists. Orphaned version histories will be skipped.");
                    versionCopyConfiguration.setCopyOrphanedVersions(null);
                }
                copyState();
            }
            if (verify || onlyVerify) {
                verify();
            }

        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private void removeCheckpointReferences(NodeBuilder builder) throws CommitFailedException {
        // removing references to the checkpoints,
        // which don't exist in the new repository
        builder.setChildNode(":async");
    }

    private void copyState() throws CommitFailedException, RepositoryException {
        final List<CommitHook> hooks = new ArrayList<>();
        if (customCommitHooks != null) {
            hooks.addAll(customCommitHooks);
        }

        boolean migrateCheckpoints = true;
        if (!isCompleteMigration()) {
            LOG.info("Checkpoints won't be migrated because of the specified paths");
            migrateCheckpoints = false;
        }
        if (!versionCopyConfiguration.isCopyAll()) {
            LOG.info("Checkpoints won't be migrated because of the specified version settings");
            migrateCheckpoints = false;
        }
        if (skipCheckpoints) {
            LOG.info("Checkpoints won't be migrated because of the --skip-checkpoints options");
            migrateCheckpoints = false;
        }
        if (migrateCheckpoints) {
            migrateCheckpoints = migrateWithCheckpoints();
        }
        if (!migrateCheckpoints) {
            NodeState sourceRoot = wrapSource(source.getRoot());
            NodeBuilder targetRoot = target.getRoot().builder();
            copyWorkspace(sourceRoot, targetRoot);
            removeCheckpointReferences(targetRoot);
            if (includeIndex) {
                IndexCopier.copy(sourceRoot, targetRoot, includePaths);
            }
            if (!versionCopyConfiguration.isCopyAll()) {
                NodeBuilder versionStorage = VersionHistoryUtil.getVersionStorage(targetRoot);
                if (!versionStorage.exists()) { // it's possible that this is a new repository and the version storage
                                                // hasn't been created/copied yet
                    versionStorage = VersionHistoryUtil.createVersionStorage(targetRoot);
                }
                if (!versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                    copyVersionStorage(targetRoot, getVersionStorage(sourceRoot), versionStorage, versionCopyConfiguration);
                }
                hooks.add(new EditorHook(new VersionableEditor.Provider(sourceRoot, getWorkspaceName(), versionCopyConfiguration)));
            }
            markIndexesToBeRebuilt(targetRoot);
            // type validation, reference and indexing hooks
            hooks.add(new EditorHook(new CompositeEditorProvider(
                    createTypeEditorProvider(),
                    createIndexEditorProvider()
            )));
            target.merge(targetRoot, new LoggingCompositeHook(hooks, null, false), CommitInfo.EMPTY);
        }
    }

    private boolean migrateWithCheckpoints() throws CommitFailedException {
        List<CheckpointRetriever.Checkpoint> checkpoints = CheckpointRetriever.getCheckpoints(source);
        if (checkpoints == null) {
            return false;
        }

        Map<String, String> nameToRevision = new LinkedHashMap<>();
        Map<String, String> checkpointSegmentToDoc = new LinkedHashMap<>();
        NodeState previousRoot = null;
        NodeBuilder targetRoot = target.getRoot().builder();
        for (CheckpointRetriever.Checkpoint checkpoint : checkpoints) {
            NodeState checkpointRoot = source.retrieve(checkpoint.getName());
            if (previousRoot == null) {
                LOG.info("Migrating first checkpoint: {}", checkpoint.getName());
                NodeStateCopier.builder()
                        .include(calculateEffectiveIncludePaths(DEFAULT_INCLUDE_PATHS, checkpointRoot))
                        .merge(of("/jcr:system"))
                        .copy(wrapSource(checkpointRoot), targetRoot);
                copyProperties(checkpointRoot, targetRoot);
            } else {
                LOG.info("Applying diff to {}", checkpoint.getName());
                checkpointRoot.compareAgainstBaseState(wrapSource(previousRoot), new ApplyDiff(targetRoot));
            }
            target.merge(targetRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            previousRoot = checkpointRoot;

            Map<String, String> checkpointInfo = source.checkpointInfo(checkpoint.getName());
            String newCheckpointName = target.checkpoint(checkpoint.getExpiryTime() - System.currentTimeMillis(), checkpointInfo);
            if (checkpointInfo.containsKey("name")) {
                nameToRevision.put(checkpointInfo.get("name"), newCheckpointName);
            }
            checkpointSegmentToDoc.put(checkpoint.getName(), newCheckpointName);
        }

        NodeState sourceRoot = source.getRoot();
        if (previousRoot == null) {
            LOG.info("No checkpoints found; migrating head");
            NodeStateCopier.builder()
                    .include(calculateEffectiveIncludePaths(DEFAULT_INCLUDE_PATHS, sourceRoot))
                    .merge(of("/jcr:system"))
                    .copy(wrapSource(sourceRoot), targetRoot);
            copyProperties(sourceRoot, targetRoot);
        } else {
            LOG.info("Applying diff to head");
            sourceRoot.compareAgainstBaseState(wrapSource(previousRoot), new ApplyDiff(targetRoot));
        }

        LOG.info("Rewriting checkpoint names in /:async {}", nameToRevision);
        NodeBuilder async = targetRoot.getChildNode(":async");
        for (Map.Entry<String, String> e : nameToRevision.entrySet()) {
            async.setProperty(e.getKey(), e.getValue(), Type.STRING);

            PropertyState temp = async.getProperty(e.getKey() + "-temp");
            if (temp == null) {
                continue;
            }
            List<String> tempValues = Lists.newArrayList(temp.getValue(Type.STRINGS));
            for (Map.Entry<String, String> sToD : checkpointSegmentToDoc.entrySet()) {
                if (tempValues.contains(sToD.getKey())) {
                    tempValues.set(tempValues.indexOf(sToD.getKey()), sToD.getValue());
                }
            }
            async.setProperty(e.getKey() + "-temp", tempValues, Type.STRINGS);
        }

        target.merge(targetRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return true;
    }

    private boolean isCompleteMigration() {
        return includePaths.equals(DEFAULT_INCLUDE_PATHS) && excludePaths.equals(DEFAULT_EXCLUDE_PATHS) && mergePaths.equals(DEFAULT_MERGE_PATHS);
    }

    private void copyWorkspace(NodeState sourceRoot, NodeBuilder targetRoot) {
        final Set<String> includes = calculateEffectiveIncludePaths(includePaths, sourceRoot);
        final Set<String> excludes;
        if (versionCopyConfiguration.isCopyAll()) {
            excludes = copyOf(this.excludePaths);
        } else {
            excludes = union(copyOf(this.excludePaths), of("/jcr:system/jcr:versionStorage"));
        }
        final Set<String> merges = union(copyOf(this.mergePaths), of("/jcr:system"));

        NodeStateCopier.builder()
            .include(includes)
            .exclude(excludes)
            .merge(merges)
            .copy(sourceRoot, targetRoot);

        if (includePaths.contains("/")) {
            copyProperties(sourceRoot, targetRoot);
        }
    }

    private String getWorkspaceName() throws RepositoryException {
        String definedName = System.getProperty(WORKSPACE_NAME_PROP);
        String detectedName = deriveWorkspaceName();
        if (StringUtils.isNotBlank(definedName)) {
            return definedName;
        } else if (StringUtils.isNotBlank(detectedName)) {
            return detectedName;
        } else {
            throw new RepositoryException("Can't detect the workspace name. Please use the system property " + WORKSPACE_NAME_PROP + " to set it manually.");
        }
    }

    /**
     * This method tries to derive the workspace name from the source repository. It uses the
     * fact that the /jcr:system/rep:permissionStore usually contains just one child
     * named after the workspace.
     *
     * @return the workspace name or null if it can't be derived
     */
    private String deriveWorkspaceName() {
        NodeState permissionStore = source.getRoot().getChildNode(JCR_SYSTEM).getChildNode(REP_PERMISSION_STORE);
        List<String> nameCandidates = new ArrayList<String>();
        for (ChildNodeEntry e : permissionStore.getChildNodeEntries()) {
            String primaryType = e.getNodeState().getName(JCR_PRIMARYTYPE);
            if (NT_REP_PERMISSION_STORE.equals(primaryType)) {
                nameCandidates.add(e.getName());
            }
        }
        if (nameCandidates.size() == 1) {
            return nameCandidates.get(0);
        } else {
            return null;
        }
    }

    private void verify() {
        final NodeState sourceRoot;
        final NodeState targetRoot;

        if (source instanceof TarNodeStore && target instanceof TarNodeStore) {
            sourceRoot = ((TarNodeStore) source).getSuperRoot();
            targetRoot = ((TarNodeStore) target).getSuperRoot();
        } else {
            sourceRoot = source.getRoot();
            targetRoot = target.getRoot();
        }

        final NodeState reportingSource = ReportingNodeState.wrap(sourceRoot, new LoggingReporter(LOG, "Verifying", LOG_NODE_COPY, -1));

        LOG.info("Verifying whether repositories are identical");
        if (targetRoot.compareAgainstBaseState(reportingSource, new LoggingEqualsDiff(LOG, "/"))) {
            LOG.info("Verification result: both repositories are identical");
        } else {
            LOG.warn("Verification result: repositories are not identical");
        }
    }

    private NodeState wrapSource(NodeState source) {
        final NodeState reportingSourceRoot = ReportingNodeState.wrap(source, new LoggingReporter(LOG, "Copying", LOG_NODE_COPY, -1));
        final NodeState sourceRoot;
        if (filterLongNames) {
            sourceRoot = NameFilteringNodeState.wrap(reportingSourceRoot);
        } else {
            sourceRoot = reportingSourceRoot;
        }
        return sourceRoot;
    }
}
