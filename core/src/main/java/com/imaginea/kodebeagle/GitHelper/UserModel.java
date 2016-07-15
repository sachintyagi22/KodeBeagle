package com.imaginea.kodebeagle.GitHelper;

import java.io.Serializable;
import java.security.Principal;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.imaginea.kodebeagle.GitHelper.Constants.AccessPermission;
import com.imaginea.kodebeagle.GitHelper.Constants.AccountType;

/**
 * UserModel is a serializable model class that represents a user and the user's
 * restricted repository memberships. Instances of UserModels are also used as
 * servlet user principals.
 *
 */
public class UserModel implements Principal, Serializable, Comparable<UserModel> {

	private static final long serialVersionUID = 1L;

	public static final UserModel ANONYMOUS = new UserModel();

	// field names are reflectively mapped in EditUser page
	public String username;
	public String password;
	public String cookie;
	public String displayName;
	public String emailAddress;
	public String organizationalUnit;
	public String organization;
	public String locality;
	public String stateProvince;
	public String countryCode;
	public boolean canAdmin;
	public boolean canFork;
	public boolean canCreate;
	public boolean excludeFromFederation;
	public boolean disabled;
	// retained for backwards-compatibility with RPC clients
	@Deprecated
	public final Set<String> repositories = new HashSet<String>();
	public final Map<String, AccessPermission> permissions = new LinkedHashMap<String, AccessPermission>();
	public final Set<TeamModel> teams = new TreeSet<TeamModel>();

	// non-persisted fields
	public boolean isAuthenticated;
	public AccountType accountType;

	public UserModel(String username) {
		this.username = username;
		this.isAuthenticated = true;
		this.accountType = AccountType.LOCAL;
	}

	private UserModel() {
		this.username = "$anonymous";
		this.isAuthenticated = false;
		this.accountType = AccountType.LOCAL;
	}

	/**
	 * Returns true if the user has any type of specified access permission for
	 * this repository.
	 *
	 * @param name
	 * @return true if user has a specified access permission for the repository
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
	 * Returns true if the user has an explicitly specified access permission
	 * for this repository.
	 *
	 * @param name
	 * @return if the user has an explicitly specified access permission
	 */
	public boolean hasExplicitRepositoryPermission(String name) {
		String repository = AccessPermission.repositoryFromRole(name).toLowerCase();
		return permissions.containsKey(repository);
	}

	/**
	 * Returns true if the user's team memberships specify an access permission
	 * for this repository.
	 *
	 * @param name
	 * @return if the user's team memberships specifi an access permission
	 */
	public boolean hasTeamRepositoryPermission(String name) {
		if (teams != null) {
			for (TeamModel team : teams) {
				if (team.hasRepositoryPermission(name)) {
					return true;
				}
			}
		}
		return false;
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

	public AccessPermission removeRepositoryPermission(String name) {
		String repository = AccessPermission.repositoryFromRole(name).toLowerCase();
		repositories.remove(repository);
		return permissions.remove(repository);
	}

	public void setRepositoryPermission(String repository, AccessPermission permission) {
		if (permission == null) {
			// remove the permission
			permissions.remove(repository.toLowerCase());
		} else {
			// set the new permission
			permissions.put(repository.toLowerCase(), permission);
		}
	}

	public boolean isAuthenticated() {
		return !UserModel.ANONYMOUS.equals(this) && isAuthenticated;
	}

	public boolean isTeamMember(String teamname) {
		for (TeamModel team : teams) {
			if (team.name.equalsIgnoreCase(teamname)) {
				return true;
			}
		}
		return false;
	}

	public TeamModel getTeam(String teamname) {
		if (teams == null) {
			return null;
		}
		for (TeamModel team : teams) {
			if (team.name.equalsIgnoreCase(teamname)) {
				return team;
			}
		}
		return null;
	}

	@Override
	public String getName() {
		return username;
	}

	public String getDisplayName() {
		if (StringUtils.isEmpty(displayName)) {
			return username;
		}
		return displayName;
	}

	@Override
	public int hashCode() {
		return username.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof UserModel) {
			return username.equals(((UserModel) o).username);
		}
		return false;
	}

	@Override
	public String toString() {
		return username;
	}

	@Override
	public int compareTo(UserModel o) {
		return username.compareTo(o.username);
	}

	/**
	 * Returns true if the name/email pair match this user account.
	 *
	 * @param name
	 * @param email
	 * @return true, if the name and email address match this account
	 */
	public boolean is(String name, String email) {
		// at a minimum a username or display name AND email address must be
		// supplied
		if (StringUtils.isEmpty(name) || StringUtils.isEmpty(email)) {
			return false;
		}
		boolean nameVerified = name.equalsIgnoreCase(username) || name.equalsIgnoreCase(getDisplayName());
		boolean emailVerified = false;
		if (StringUtils.isEmpty(emailAddress)) {
			// user account has not specified an email address
			// fail
			emailVerified = false;
		} else {
			// user account has specified an email address
			emailVerified = email.equalsIgnoreCase(emailAddress);
		}
		return nameVerified && emailVerified;
	}

}
