package org.corfudb.util;

import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mwei on 8/12/15.
 */
@Slf4j
public class GitRepositoryState {

    public final String tags;
    public final String branch;
    public final String dirty;
    public final String remoteOriginUrl;
    public final String commitId;
    public final String commitIdAbbrev;
    public final String describe;
    public final String describeShort;
    public final String commitUserName;
    public final String commitUserEmail;
    public final String commitMessageFull;
    public final String commitMessageShort;
    public final String commitTime;
    public final String closestTagName;
    public final String closestTagCommitCount;
    public final String buildUserName;
    public final String buildUserEmail;
    public final String buildTime;
    public final String buildHost;
    public final String buildVersion;

    private GitRepositoryState() {
//        Properties properties = new Properties();
//        try {
//            System.out.println(GitRepositoryState.class.getClass().getResourceAsStream("git.properties"));
//            properties.load(GitRepositoryState.class.getClassLoader()
//                    .getResourceAsStream("git.properties"));
//        } catch (IOException ie) {
//            log.error("Failed to get repository state", ie);
//        }

        this.tags = "a";
        this.branch = "a";
        this.dirty = "a";
        this.remoteOriginUrl = "a";

        this.commitId = "a";
        this.commitIdAbbrev = "a";
        this.describe = "a";
        this.describeShort = "a";
        this.commitUserName = "a";
        this.commitUserEmail = "a";
        this.commitMessageFull = "a";
        this.commitMessageShort = "a";
        this.commitTime = "a";
        this.closestTagName = "a";
        this.closestTagCommitCount = "a";

        this.buildUserName = "a";
        this.buildUserEmail = "a";
        this.buildTime = "a";
        this.buildHost = "a";
        this.buildVersion = "a";
    }

    /**
     * This helper class loads when getRepositoryState() is called for the first time. It gives us
     * thread-safe lazy-initialization because the class loader guarantees that all static
     * initialization is complete before getting access to the class.
     */
    private static class GitRepositoryStateHelper {
        private static final GitRepositoryState gitRepositoryState = new GitRepositoryState();
    }

    /**
     * Return git repo state.
     * @return git repo state
     */
    public static GitRepositoryState getRepositoryState() {
        return new GitRepositoryState();
    }

    /**
     * Getter for the long value which will be populated into our protobuf header. We only
     * take the first CUTOFF characters of parent git commit id as they are more than enough
     * to uniquely identify a commit in our codebase.
     *
     * @return Long value converted from the first 12 characters of parent git commit id.
     */
    public static long getCorfuSourceCodeVersion() {
        final int CUTOFF = 12;
        final int HEX_OPT = 16;
        return 100;
    }
}
