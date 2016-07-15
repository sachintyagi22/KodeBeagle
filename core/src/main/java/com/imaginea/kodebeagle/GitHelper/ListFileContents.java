package com.imaginea.kodebeagle.GitHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.errors.LargeObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevObject;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;

import com.imaginea.kodebeagle.GitHelper.PathModel.PathChangeModel;

public class ListFileContents {

	/**
	 */
	public static Map<String, Integer> getNRelatedFiles(Repository repository, String filePath, int n)
			throws GitAPIException, IOException {

		filePath = "core/src/main/scala/com/kodebeagle/crawler/ZipBasicParser.scala";

		Map<String, Integer> fileRankings = new HashMap<String, Integer>();

		walkAllCommits(repository, fileRankings, filePath);

		Map<String, Integer> topNFiles = SortRankings.sortRankingsMap(fileRankings, n);
		return topNFiles;
	}

	/**
	 * This method uses RevWalk to quickly iterate over all available commits
	 * 
	 * @param repo
	 * @throws GitAPIException
	 * @throws IOException
	 */
	public static void walkAllCommits(Repository repo, Map<String, Integer> fileRankings, String fileName)
			throws GitAPIException, IOException {

		Git git = new Git(repo);
		RevWalk walk = new RevWalk(repo);
		List<Ref> branches = git.branchList().call();

		for (Ref branch : branches) {
			String branchName = branch.getName();

			Iterable<RevCommit> commits = git.log().all().call();

			for (RevCommit commit : commits) {
				boolean foundInThisBranch = false;

				RevCommit targetCommit = walk.parseCommit(repo.resolve(commit.getName()));
				for (Entry<String, Ref> e : repo.getAllRefs().entrySet()) {
					if (e.getKey().startsWith("refs/heads/")) {
						if (walk.isMergedInto(targetCommit, walk.parseCommit(e.getValue().getObjectId()))) {
							String foundInBranch = e.getValue().getName();
							if (branchName.equals(foundInBranch)) {
								foundInThisBranch = true;
								break;
							}
						}
					}
				}
				if (foundInThisBranch) {
					List<PathChangeModel> filesList = getFilesInCommit(repo, commit, true);
					for (int i = 0; i < filesList.size(); i++) {
						if (filesList.get(i).name.equals(fileName)) {
							rankFilesCommitLevel(filesList, fileRankings, fileName);
						} else {
							// ignore as of now placeholder
						}
					}
				}
			}
		}
		git.close();

	}

	/**
	 * Ranks all files based on the availability in previous commits
	 * 
	 * @param filesList
	 * @param fileRankings
	 */
	public static void rankFilesCommitLevel(List<PathChangeModel> filesList, Map<String, Integer> fileRankings,
			String fileName) {
		for (int i = 0; i < filesList.size(); i++) {
			String key = filesList.get(i).name;
			int count = fileRankings.containsKey(key) ? fileRankings.get(key) : 0;
			fileRankings.put(key, count + 1);
		}
		fileRankings.remove(fileName);
	}

	/**
	 * Returns the displayable name of the person in the form "Real Name <email
	 * address>". If the email address is empty, just "Real Name" is returned.
	 *
	 * @param person
	 * @return "Real Name <email address>" or "Real Name"
	 */

	public static String getDisplayName(PersonIdent person) {
		if (StringUtils.isEmpty(person.getEmailAddress())) {
			return person.getName();
		}
		final StringBuilder r = new StringBuilder();
		r.append(person.getName());
		r.append(" <");
		r.append(person.getEmailAddress());
		r.append('>');
		return r.toString().trim();
	}

	/**
	 * Retrieves a Java Date from a Git commit.
	 *
	 * @param commit
	 * @return date of the commit or Date(0) if the commit is null
	 */

	public static Date getAuthorDate(RevCommit commit) {
		if (commit == null) {
			return new Date(0);
		}
		if (commit.getAuthorIdent() != null) {
			return commit.getAuthorIdent().getWhen();
		}
		return getCommitDate(commit);
	}

	/**
	 * Retrieves a Java Date from a Git commit.
	 *
	 * @param commit
	 * @return date of the commit or Date(0) if the commit is null
	 */
	public static Date getCommitDate(RevCommit commit) {
		if (commit == null) {
			return new Date(0);
		}
		return new Date(commit.getCommitTime() * 1000L);
	}

	/**
	 * Returns the list of local branches in the repository. If repository does
	 * not exist or is empty, an empty list is returned.
	 *
	 * @param repository
	 * @param fullName
	 *            if true, /refs/heads/yadayadayada is returned. If false,
	 *            yadayadayada is returned.
	 * @param maxCount
	 *            if < 0, all local branches are returned
	 * @return list of local branches
	 */

	public static List<RefModel> getLocalBranches(Repository repository, boolean fullName, int maxCount) {
		return getRefs(repository, Constants.R_HEADS, fullName, maxCount);
	}

	/**
	 * Returns the list of refs in the specified base ref. If repository does
	 * not exist or is empty, an empty list is returned.
	 *
	 * @param repository
	 * @param fullName
	 *            if true, /refs/yadayadayada is returned. If false,
	 *            yadayadayada is returned.
	 * @return list of refs
	 */
	private static List<RefModel> getRefs(Repository repository, String refs, boolean fullName, int maxCount) {
		return getRefs(repository, refs, fullName, maxCount, 0);
	}

	/**
	 * Returns a list of references in the repository matching "refs". If the
	 * repository is null or empty, an empty list is returned.
	 *
	 * @param repository
	 * @param refs
	 *            if unspecified, all refs are returned
	 * @param fullName
	 *            if true, /refs/something/yadayadayada is returned. If false,
	 *            yadayadayada is returned.
	 * @param maxCount
	 *            if < 0, all references are returned
	 * @return list of references
	 */
	private static List<RefModel> getRefs(Repository repository, String refs, boolean fullName, int maxCount,
			int offset) {
		List<RefModel> list = new ArrayList<RefModel>();
		if (maxCount == 0) {
			return list;
		}
		if (!hasCommits(repository)) {
			return list;
		}
		try {
			Map<String, Ref> map = repository.getRefDatabase().getRefs(refs);
			RevWalk rw = new RevWalk(repository);
			for (Entry<String, Ref> entry : map.entrySet()) {
				Ref ref = entry.getValue();
				RevObject object = rw.parseAny(ref.getObjectId());
				String name = entry.getKey();
				if (fullName && !StringUtils.isEmpty(refs)) {
					name = refs + name;
				}
				list.add(new RefModel(name, ref, object));
			}
			rw.dispose();
			Collections.sort(list);
			Collections.reverse(list);
			if (maxCount > 0 && list.size() > maxCount) {
				if (offset < 0) {
					offset = 0;
				}
				int endIndex = offset + maxCount;
				if (endIndex > list.size()) {
					endIndex = list.size();
				}
				list = new ArrayList<RefModel>(list.subList(offset, endIndex));
			}
		} catch (IOException e) {
			error(e, repository, "{0} failed to retrieve {1}", refs);
		}
		return list;
	}

	/**
	 * Log an error message and exception.
	 *
	 * @param t
	 * @param repository
	 *            if repository is not null it MUST be the {0} parameter in the
	 *            pattern.
	 * @param pattern
	 * @param objects
	 */

	private static void error(Throwable t, Repository repository, String pattern, Object... objects) {
		List<Object> parameters = new ArrayList<Object>();
		if (objects != null && objects.length > 0) {
			for (Object o : objects) {
				parameters.add(o);
			}
		}
		if (repository != null) {
			parameters.add(0, repository.getDirectory().getAbsolutePath());
		}
	}

	/**
	 * Returns the default branch to use for a repository. Normally returns
	 * whatever branch HEAD points to, but if HEAD points to nothing it returns
	 * the most recently updated branch.
	 *
	 * @param repository
	 * @return the objectid of a branch
	 * @throws Exception
	 */
	public static ObjectId getDefaultBranch(Repository repository) throws Exception {
		ObjectId object = repository.resolve(Constants.HEAD);
		if (object == null) {
			// no HEAD
			// perhaps non-standard repository, try local branches
			List<RefModel> branchModels = getLocalBranches(repository, true, -1);
			if (branchModels.size() > 0) {
				// use most recently updated branch
				RefModel branch = null;
				Date lastDate = new Date(0);
				for (RefModel branchModel : branchModels) {
					if (branchModel.getDate().after(lastDate)) {
						branch = branchModel;
						lastDate = branch.getDate();
					}
				}
				object = branch.getReferencedObjectId();
			}
		}
		return object;
	}

	public static boolean isPossibleFilestoreItem(long size) {
		return ((size >= 125) && (size <= 146));
	}

	/**
	 * 
	 * @return Representative FilestoreModel if valid, otherwise null
	 */
	public static FilestoreModel getFilestoreItem(ObjectLoader obj) {
		try {
			final byte[] blob = obj.getCachedBytes(146);
			final String meta = new String(blob, "UTF-8");

			return FilestoreModel.fromMetaString(meta);

		} catch (LargeObjectException e) {
			// Intentionally failing silent
		} catch (Exception e) {
			error(e, null, "failed to retrieve filestoreItem " + obj.toString());
		}

		return null;
	}

	/**
	 * Returns the list of files changed in a specified commit. If the
	 * repository does not exist or is empty, an empty list is returned.
	 *
	 * @param repository
	 * @param commit
	 *            if null, HEAD is assumed.
	 * @param calculateDiffStat
	 *            if true, each PathChangeModel will have insertions/deletions
	 * @return list of files changed in a commit
	 */

	public static List<PathChangeModel> getFilesInCommit(Repository repository, RevCommit commit,
			boolean calculateDiffStat) throws IOException {

		List<PathChangeModel> list = new ArrayList<>();

		if (!hasCommits(repository)) {
			System.out.println("No commits in Repository");
			return list;
		}

		RevWalk rw = new RevWalk(repository);

		try {
			if (commit == null) {
				ObjectId object = getDefaultBranch(repository);
				commit = rw.parseCommit(object);
			}

			if (commit.getParentCount() == 0) {
				TreeWalk tw = new TreeWalk(repository);
				tw.reset();
				tw.setRecursive(true);
				tw.addTree(commit.getTree());
				while (tw.next()) {
					long size = 0;
					FilestoreModel filestoreItem = null;
					ObjectId objectId = tw.getObjectId(0);

					try {
						if (!tw.isSubtree() && (tw.getFileMode(0) != FileMode.GITLINK)) {

							size = tw.getObjectReader().getObjectSize(objectId, Constants.OBJ_BLOB);

							if (isPossibleFilestoreItem(size)) {
								filestoreItem = getFilestoreItem(tw.getObjectReader().open(objectId));
							}
						}
					} catch (Throwable t) {
						error(t, null, "failed to retrieve blob size for " + tw.getPathString());
					}

					list.add(new PathChangeModel(tw.getPathString(), tw.getPathString(), filestoreItem, size,
							tw.getRawMode(0), objectId.getName(), commit.getId().getName(), ChangeType.ADD));
				}
			} else {
				RevCommit parent = rw.parseCommit(commit.getParent(0).getId());
				DiffStatFormatter df = new DiffStatFormatter(commit.getName(), repository);
				df.setRepository(repository);
				df.setDiffComparator(RawTextComparator.DEFAULT);
				df.setDetectRenames(true);
				List<DiffEntry> diffs = df.scan(parent.getTree(), commit.getTree());
				for (DiffEntry diff : diffs) {
					// create the path change model
					PathChangeModel pcm = PathChangeModel.from(diff, commit.getName(), repository);

					if (calculateDiffStat) {
						// update file diffstats
						df.format(diff);
						PathChangeModel pathStat = df.getDiffStat().getPath(pcm.path);
						if (pathStat != null) {
							pcm.insertions = pathStat.insertions;
							pcm.deletions = pathStat.deletions;
						}
					}
					list.add(pcm);
				}
			}
		} catch (Throwable t) {
			error(t, repository, "{0} failed to determine files in commit!");
		} finally {
			rw.dispose();
		}
		return list;

	}

	/**
	 * Determine if a repository has any commits. This is determined by checking
	 * the for loose and packed objects.
	 *
	 * @param repository
	 * @return true if the repository has commits
	 */
	public static boolean hasCommits(Repository repository) {
		if (repository != null && repository.getDirectory().exists()) {
			return (new File(repository.getDirectory(), "objects").list().length > 2)
					|| (new File(repository.getDirectory(), "objects/pack").list().length > 0);
		}
		return false;
	}

}
