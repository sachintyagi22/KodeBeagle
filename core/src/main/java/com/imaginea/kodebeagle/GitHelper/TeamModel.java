package com.imaginea.kodebeagle.GitHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.imaginea.kodebeagle.GitHelper.Constants.AccessPermission;
import com.imaginea.kodebeagle.GitHelper.Constants.AccountType;

/**
 * TeamModel is a serializable model class that represents a group of users and
 * a list of accessible repositories.
 *
 */
public class TeamModel implements Serializable, Comparable<TeamModel> {

	private static final long serialVersionUID = 1L;

	// field names are reflectively mapped in EditTeam page
	public String name;
	public boolean canAdmin;
	public boolean canFork;
	public boolean canCreate;
	public AccountType accountType;
	public final Set<String> users = new HashSet<String>();
	// retained for backwards-compatibility with RPC clients
	@Deprecated
	public final Set<String> repositories = new HashSet<String>();
	public final Map<String, AccessPermission> permissions = new LinkedHashMap<String, AccessPermission>();
	public final Set<String> mailingLists = new HashSet<String>();
	public final List<String> preReceiveScripts = new ArrayList<String>();
	public final List<String> postReceiveScripts = new ArrayList<String>();

	public TeamModel(String name) {
		this.name = name;
		this.accountType = AccountType.LOCAL;
	}

	/**
	 * Returns true if the team has any type of specified access permission for
	 * this repository.
	 *
	 * @param name
	 * @return true if team has a specified access permission for the repository
	 */
	public boolean hasRepositoryPermission(String name) {
		String repository = AccessPermission.repositoryFromRole(name).toLowerCase();
		if (permissions.containsKey(repository)) {
			// exact repository permission specified
			return true;
		} else {
			// search for regex permission match
			for (String key : permissions.keySet()) {
				if (name.matches(key)) {
					AccessPermission p = permissions.get(key);
					if (p != null) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * Returns true if the team has an explicitly specified access permission
	 * for this repository.
	 *
	 * @param name
	 * @return if the team has an explicitly specified access permission
	 */
	public boolean hasExplicitRepositoryPermission(String name) {
		String repository = AccessPermission.repositoryFromRole(name).toLowerCase();
		return permissions.containsKey(repository);
	}

	/**
	 * Adds a repository permission to the team.
	 * <p>
	 * Role may be formatted as:
	 * <ul>
	 * <li>myrepo.git <i>(this is implicitly RW+)</i>
	 * <li>RW+:myrepo.git
	 * </ul>
	 * 
	 * @param role
	 */
	public void addRepositoryPermission(String role) {
		AccessPermission permission = AccessPermission.permissionFromRole(role);
		String repository = AccessPermission.repositoryFromRole(role).toLowerCase();
		repositories.add(repository);
		permissions.put(repository, permission);
	}

	public void addRepositoryPermissions(Collection<String> roles) {
		for (String role : roles) {
			addRepositoryPermission(role);
		}
	}

	public AccessPermission removeRepositoryPermission(String name) {
		String repository = AccessPermission.repositoryFromRole(name).toLowerCase();
		repositories.remove(repository);
		return permissions.remove(repository);
	}

	public void setRepositoryPermission(String repository, AccessPermission permission) {
		if (permission == null) {
			// remove the permission
			permissions.remove(repository.toLowerCase());
			repositories.remove(repository.toLowerCase());
		} else {
			// set the new permission
			permissions.put(repository.toLowerCase(), permission);
			repositories.add(repository.toLowerCase());
		}
	}

	public boolean hasUser(String name) {
		return users.contains(name.toLowerCase());
	}

	public void addUser(String name) {
		users.add(name.toLowerCase());
	}

	public void addUsers(Collection<String> names) {
		for (String name : names) {
			users.add(name.toLowerCase());
		}
	}

	public void removeUser(String name) {
		users.remove(name.toLowerCase());
	}

	public void addMailingLists(Collection<String> addresses) {
		for (String address : addresses) {
			mailingLists.add(address.toLowerCase());
		}
	}

	public boolean isLocalTeam() {
		return accountType.isLocal();
	}

	@Override
	public String toString() {
		return name;
	}

	@Override
	public int compareTo(TeamModel o) {
		return name.compareTo(o.name);
	}
}
