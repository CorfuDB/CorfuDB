package org.corfudb.util;

import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 8/12/15.
 */
@Slf4j
public class GitRepositoryState {

    private static GitRepositoryState _gitRepositoryState = null;

    public String tags;
    public String branch;
    public String dirty;
    public String remoteOriginUrl;
    public String commitId;
    public String commitIdAbbrev;
    public String describe;
    public String describeShort;
    public String commitUserName;
    public String commitUserEmail;
    public String commitMessageFull;
    public String commitMessageShort;
    public String commitTime;
    public String closestTagName;
    public String closestTagCommitCount;
    public String buildUserName;
    public String buildUserEmail;
    public String buildTime;
    public String buildHost;
    public String buildVersion;

    private GitRepositoryState(Properties properties) {
        this.tags = properties.get("git.tags").toString();
        this.branch = properties.get("git.branch").toString();
        this.dirty = properties.get("git.dirty").toString();
        this.remoteOriginUrl = properties.get("git.remote.origin.url").toString();

        this.commitId = properties.get("git.commit.id").toString();
        this.commitIdAbbrev = properties.get("git.commit.id.abbrev").toString();
        this.describe = properties.get("git.commit.id.describe").toString();
        this.describeShort = properties.get("git.commit.id.describe-short").toString();
        this.commitUserName = properties.get("git.commit.user.name").toString();
        this.commitUserEmail = properties.get("git.commit.user.email").toString();
        this.commitMessageFull = properties.get("git.commit.message.full").toString();
        this.commitMessageShort = properties.get("git.commit.message.short").toString();
        this.commitTime = properties.get("git.commit.time").toString();
        this.closestTagName = properties.get("git.closest.tag.name").toString();
        this.closestTagCommitCount = properties.get("git.closest.tag.commit.count").toString();

        this.buildUserName = properties.get("git.build.user.name").toString();
        this.buildUserEmail = properties.get("git.build.user.email").toString();
        this.buildTime = properties.get("git.build.time").toString();
        this.buildHost = properties.get("git.build.host").toString();
        this.buildVersion = properties.get("git.build.version").toString();
    }

    /**
     * Return git repo state.
     * @return git repo state
     */
    public static GitRepositoryState getRepositoryState() {
        if (_gitRepositoryState == null) {
            Properties properties = new Properties();
            try {
                properties.load(GitRepositoryState.class.getClassLoader()
                        .getResourceAsStream("git.properties"));
            } catch (IOException ie) {
                log.error("Failed to get repository state", ie);
            }
            _gitRepositoryState = new GitRepositoryState(properties);
        }
        return _gitRepositoryState;
    }

}
