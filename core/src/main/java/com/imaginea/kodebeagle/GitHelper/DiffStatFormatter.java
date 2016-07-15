package com.imaginea.kodebeagle.GitHelper;

import java.io.IOException;

import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.util.io.NullOutputStream;

import com.imaginea.kodebeagle.GitHelper.DiffUtils.DiffStat;



/**
 * Calculates a DiffStat.
 *
 */
public class DiffStatFormatter extends DiffFormatter {

	private final DiffStat diffStat;

	private PathModel path;

	public DiffStatFormatter(String commitId, Repository repository) {
		super(NullOutputStream.INSTANCE);
		diffStat = new DiffStat(commitId, repository);
	}

	@Override
	public void format(DiffEntry entry) throws IOException {
		path = diffStat.addPath(entry);
		super.format(entry);
	}

	public DiffStat getDiffStat() {
		return diffStat;
	}
}
