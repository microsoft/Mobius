Contributing Code Changes
=========================
This page documents the various steps required in order to contribute Mobius code changes. 

### Overview
Generally, Mobius uses:
* [Github issues](https://github.com/Microsoft/SparkCLR/issues) to track logical issues, including bugs and improvements
* [Github pull requests](https://github.com/Microsoft/SparkCLR/pulls) to manage the *code review* and merge of *code changes*.

### Github Issues
[Issue Guide](issue-guide.md) explains Github labels used for managing Mobius *issues*. Even though Github allows only committers to apply labels, **Prefix titles with labels** section explains how to lable an *issue*. The steps below help you assess whether and how to file a *Github issue*, before a *pull request*:
  
1. Find the existing *Github issue* that the *code change* pertains to.
  1. Do not create a new *Github issue* for creating a *code change* to address an existing *issue*; reuse the existing *issue* instead.
  2. To avoid conflicts, assign the *Github issue* to yourself if you plan to work on it, add the *Ownership* label of `grabbed by the assignee`, and corresponding *Project Management* label, either `Backlog`, `Up Next`, or `In Progress` (see [Issue Guide](issue-guide.md) for more details).
  3. Look for existing *pull requests* that are linked from the *issue*, to understand if someone is already working on it.
2. If the change is new, then it usually needs a new *Github issue*. However, *trivial changes*, where *"what should change"* is virtually the same as *"how it should change"*, do not require a *Github issue*. Example: *"Fix typos in Fooo scaladoc"*
3. If required, create a new *Github issue*:
  1. Provide a descriptive Title. *"Update Checkpoint API"* or *"Problem in DataFrame API"* is not sufficient. *"Checkpoint API fails to save data to HDFS"* is good.
  2. Write a detailed Description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a *design document*, especially if it's a *major change*.
  3. Set proper issue *Type* and *Area* labels, per [Issue Guide](issue-guide.md). 
  4. To avoid conflicts, assign the *Github issue* to yourself if you plan to work on it, add the *Ownership* label of `grabbed by the assignee`, and corresponding *Project Management* label, either `Backlog`, `Up Next`, or `In Progress`. Leave it unassigned otherwise.
  5. Do not include a patch file; pull requests are used to propose the actual change.
  6. If the change is a major one, consider inviting discussion on the issue at *[sparkclr-dev](https://groups.google.com/d/forum/sparkclr-dev)* mailing list first before proceeding to implement the change. Note that a design doc helps the discussion and the review of *major* changes.

### Pull Request
1. Fork the Github repository at http://github.com/Microsoft/SparkCLR if you haven't already.
2. Clone your fork, create a new dev branch, push commits to the dev branch.
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed (doc changes should be submitted along with code change in the same PR).
4. Run all tests and samples as described in the project's [README](../../README.md).
5. Open a *pull request* against the master branch of Microsoft/SparkCLR. (Only in special cases would the PR be opened against other branches.)
  1. Always associate the PR with corresponding *Github issues* execpt for trial changes when no *Github issue* is created.
  2. For trivial cases where an *Github issue* is not required, **MINOR:** or **HOTFIX:** can be used as the PR title prefix.
  3. If the *pull request* is still a work in progress, not ready to be merged, but needs to be pushed to Github to facilitate review, then prefix the PR title with **[WIP]**.
  4. Consider identifying committers or other contributors who have worked on the code being changed. Find the file(s) in Github and click *"Blame"* to see a line-by-line annotation of who changed the code last. You can add `@username` in the PR description to ping them immediately.
6. Investigate and fix failures caused by the pull request:
  1. Fixes can simply be pushed to the same branch from which you opened your pull request.
  2. If the failure is unrelated to your pull request and you have been able to run the tests locally successfully, please mention it in the pull request.

### The Review Process
1. Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
2. Lively, polite, rapid technical debate is encouraged from everyone in the community. The outcome may be a rejection of the entire change.
3. Reviewers can indicate that a change looks suitable for merging with a comment such as: *"I think this patch looks good"*. Mobius uses the **LGTM** convention for indicating the strongest level of technical sign-off on a patch: simply comment with the word **LGTM**. It specifically means: *"I've looked at this thoroughly and take as much ownership as if I wrote the patch myself"*. If you comment **LGTM** you will be expected to help with bugs or follow-up issues on the patch. Consistent, judicious use of **LGTM** is a great way to gain credibility as a reviewer with the broader community.
4. The *Github issue* should be labelled as `In Progress` if the pull request needs more work.
5. Sometimes, other changes will be merged which conflict with your pull request's changes. The PR can't be merged until the conflict is resolved. This can be resolved with `git fetch origin` followed by `git rebase origin/master` and resolving the conflicts by hand, then pushing the result to your branch.
6. Try to be responsive to the discussion rather than let days pass between replies.

### Closing Your Pull Request / Github Issue
1. If a change is accepted, it will be merged and the *pull request* will automatically be closed, along with the associated *Github issues* if any.
  1. Note that in the rare case you are asked to open a pull request against a branch besides *master*, that you will actually have to close the pull request manually
  2. The *Github issue* will be Assigned to the primary contributor to the change as a way of giving credit. If the *issue* isn't closed and/or Assigned promptly, comment on the *issue*.
2. It is the **PR submitter's responsibility** to resolve any **merge conflicts**. This applies to the situation where two PRs change the same code fragment – second merge will fail and the submitter should be *politely* asked to fix the conflicts – it is just an accepted fact of live occasionally with distributed development process on GitHub, so no malice involved – *"First merge wins"*.
3. If your pull request is ultimately rejected, please close it promptly
  * ... because committers can't close PRs directly
3. If a *pull request* has gotten little or no attention, consider improving the description or the change itself and ping likely reviewers again after a few days. Consider proposing a change that's easier to include, like a smaller and/or less invasive change.
4. If a pull request is closed because it is deemed not the right approach to resolve a *Github issue*, then leave the *issue* open. However if the review makes it clear that the problem identified in the *Github issue* is not going to be resolved by any pull request (not a problem, *won't fix*) then also resolve the Github *issue*.

==============================
**Credit** to the [Apache Kafka](https://cwiki.apache.org/confluence/display/KAFKA/Contributing+Code+Changes). We are borrowing liberally from their process.

There may be bugs or possible improvements to this page, so help us improve it.
