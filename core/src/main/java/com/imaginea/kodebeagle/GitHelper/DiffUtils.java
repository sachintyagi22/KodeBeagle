package com.imaginea.kodebeagle.GitHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imaginea.kodebeagle.GitHelper.PathModel.PathChangeModel;

/**
 * DiffUtils is a class of utility methods related to diff, patch, and blame.
 *
 * The diff methods support pluggable diff output types like Gitblit, Gitweb,
 * and Plain.
 *
 *
 */
public class DiffUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(DiffUtils.class);

	/**
	 * Callback interface for binary diffs. All the getDiff methods here take an
	 * optional handler; if given and the {@link DiffOutputType} is
	 * {@link DiffOutputType#HTML HTML}, it is responsible for displaying a
	 * binary diff.
	 */
	public interface BinaryDiffHandler {

		/**
		 * Renders a binary diff. The result must be valid HTML, it will be
		 * inserted into an HTML table cell. May return {@code null} if the
		 * default behavior (which is typically just a textual note "Bnary files
		 * differ") is desired.
		 *
		 * @param diffEntry
		 *            current diff entry
		 *
		 * @return the rendered diff as HTML, or {@code null} if the default is
		 *         desired.
		 */
		public String renderBinaryDiff(final DiffEntry diffEntry);

	}

	/**
	 * Enumeration for the diff output types.
	 */
	public static enum DiffOutputType {
		PLAIN, HTML;

		public static DiffOutputType forName(String name) {
			for (DiffOutputType type : values()) {
				if (type.name().equalsIgnoreCase(name)) {
					return type;
				}
			}
			return null;
		}
	}

	/**
	 * Enumeration for the diff comparator types.
	 */
	public static enum DiffComparator {
		SHOW_WHITESPACE(RawTextComparator.DEFAULT), IGNORE_WHITESPACE(RawTextComparator.WS_IGNORE_ALL), IGNORE_LEADING(
				RawTextComparator.WS_IGNORE_LEADING), IGNORE_TRAILING(
						RawTextComparator.WS_IGNORE_TRAILING), IGNORE_CHANGES(RawTextComparator.WS_IGNORE_CHANGE);

		public final RawTextComparator textComparator;

		DiffComparator(RawTextComparator textComparator) {
			this.textComparator = textComparator;
		}

		public DiffComparator getOpposite() {
			return this == SHOW_WHITESPACE ? IGNORE_WHITESPACE : SHOW_WHITESPACE;
		}

		public String getTranslationKey() {
			return "gb." + name().toLowerCase();
		}

		public static DiffComparator forName(String name) {
			for (DiffComparator type : values()) {
				if (type.name().equalsIgnoreCase(name)) {
					return type;
				}
			}
			return null;
		}
	}

	/**
	 * Encapsulates the output of a diff.
	 */
	public static class DiffOutput implements Serializable {
		private static final long serialVersionUID = 1L;

		public final DiffOutputType type;
		public final String content;
		public final DiffStat stat;

		DiffOutput(DiffOutputType type, String content, DiffStat stat) {
			this.type = type;
			this.content = content;
			this.stat = stat;
		}

		public PathChangeModel getPath(String path) {
			if (stat == null) {
				return null;
			}
			return stat.getPath(path);
		}
	}

	/**
	 * Class that represents the number of insertions and deletions from a
	 * chunk.
	 */
	public static class DiffStat implements Serializable {

		private static final long serialVersionUID = 1L;

		public final List<PathChangeModel> paths = new ArrayList<PathChangeModel>();

		private final String commitId;

		private final Repository repository;

		public DiffStat(String commitId, Repository repository) {
			this.commitId = commitId;
			this.repository = repository;
		}

		public PathChangeModel addPath(DiffEntry entry) {
			PathChangeModel pcm = PathChangeModel.from(entry, commitId, repository);
			paths.add(pcm);
			return pcm;
		}

		public int getInsertions() {
			int val = 0;
			for (PathChangeModel entry : paths) {
				val += entry.insertions;
			}
			return val;
		}

		public int getDeletions() {
			int val = 0;
			for (PathChangeModel entry : paths) {
				val += entry.deletions;
			}
			return val;
		}

		public PathChangeModel getPath(String path) {
			PathChangeModel stat = null;
			for (PathChangeModel p : paths) {
				if (p.path.equals(path)) {
					stat = p;
					break;
				}
			}
			return stat;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (PathChangeModel entry : paths) {
				sb.append(entry.toString()).append('\n');
			}
			sb.setLength(sb.length() - 1);
			return sb.toString();
		}
	}

	public static class NormalizedDiffStat implements Serializable {

		private static final long serialVersionUID = 1L;

		public final int insertions;
		public final int deletions;
		public final int blanks;

		NormalizedDiffStat(int insertions, int deletions, int blanks) {
			this.insertions = insertions;
			this.deletions = deletions;
			this.blanks = blanks;
		}
	}

	/**
	 * Returns the diffstat between the two commits for the specified file or
	 * folder.
	 *
	 * @param repository
	 * @param base
	 *            if base commit is unspecified, the diffstat is generated
	 *            against the primary parent of the specified tip.
	 * @param tip
	 * @param path
	 *            if path is specified, the diffstat is generated only for the
	 *            specified file or folder. if unspecified, the diffstat is
	 *            generated for the entire diff between the two commits.
	 * @return patch as a string
	 */
	public static DiffStat getDiffStat(Repository repository, String base, String tip) {
		RevCommit baseCommit = null;
		RevCommit tipCommit = null;
		RevWalk revWalk = new RevWalk(repository);
		try {
			tipCommit = revWalk.parseCommit(repository.resolve(tip));
			if (!StringUtils.isEmpty(base)) {
				baseCommit = revWalk.parseCommit(repository.resolve(base));
			}
			return getDiffStat(repository, baseCommit, tipCommit, null);
		} catch (Exception e) {
			LOGGER.error("failed to generate diffstat!", e);
		} finally {
			revWalk.dispose();
		}
		return null;
	}

	public static DiffStat getDiffStat(Repository repository, RevCommit commit) {
		return getDiffStat(repository, null, commit, null);
	}

	/**
	 * Returns the diffstat between the two commits for the specified file or
	 * folder.
	 *
	 * @param repository
	 * @param baseCommit
	 *            if base commit is unspecified, the diffstat is generated
	 *            against the primary parent of the specified commit.
	 * @param commit
	 * @param path
	 *            if path is specified, the diffstat is generated only for the
	 *            specified file or folder. if unspecified, the diffstat is
	 *            generated for the entire diff between the two commits.
	 * @return patch as a string
	 */
	public static DiffStat getDiffStat(Repository repository, RevCommit baseCommit, RevCommit commit, String path) {
		DiffStat stat = null;
		try {
			RawTextComparator cmp = RawTextComparator.DEFAULT;
			DiffStatFormatter df = new DiffStatFormatter(commit.getName(), repository);
			df.setRepository(repository);
			df.setDiffComparator(cmp);
			df.setDetectRenames(true);

			RevTree commitTree = commit.getTree();
			RevTree baseTree;
			if (baseCommit == null) {
				if (commit.getParentCount() > 0) {
					final RevWalk rw = new RevWalk(repository);
					RevCommit parent = rw.parseCommit(commit.getParent(0).getId());
					baseTree = parent.getTree();
				} else {
					baseTree = commitTree;
				}
			} else {
				baseTree = baseCommit.getTree();
			}

			List<DiffEntry> diffEntries = df.scan(baseTree, commitTree);
			if (path != null && path.length() > 0) {
				for (DiffEntry diffEntry : diffEntries) {
					if (diffEntry.getNewPath().equalsIgnoreCase(path)) {
						df.format(diffEntry);
						break;
					}
				}
			} else {
				df.format(diffEntries);
			}
			stat = df.getDiffStat();
			df.flush();
		} catch (Throwable t) {
			LOGGER.error("failed to generate commit diff!", t);
		}
		return stat;
	}

	/**
	 * Normalizes a diffstat to an N-segment display.
	 *
	 * @params segments
	 * @param insertions
	 * @param deletions
	 * @return a normalized diffstat
	 */
	public static NormalizedDiffStat normalizeDiffStat(final int segments, final int insertions, final int deletions) {
		final int total = insertions + deletions;
		final float fi = ((float) insertions) / total;
		int si;
		int sd;
		int sb;
		if (deletions == 0) {
			// only addition
			si = Math.min(insertions, segments);
			sd = 0;
			sb = si < segments ? (segments - si) : 0;
		} else if (insertions == 0) {
			// only deletion
			si = 0;
			sd = Math.min(deletions, segments);
			sb = sd < segments ? (segments - sd) : 0;
		} else if (total <= segments) {
			// total churn fits in segment display
			si = insertions;
			sd = deletions;
			sb = segments - total;
		} else if ((segments % 2) > 0 && fi > 0.45f && fi < 0.55f) {
			// odd segment display, fairly even +/-, use even number of segments
			si = Math.round(((float) insertions) / total * (segments - 1));
			sd = segments - 1 - si;
			sb = 1;
		} else {
			si = Math.round(((float) insertions) / total * segments);
			sd = segments - si;
			sb = 0;
		}

		return new NormalizedDiffStat(si, sd, sb);
	}
}
