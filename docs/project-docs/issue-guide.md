Issue Guide
===========

This page outlines how Mobius issues are handled. 
  
*Issues on GitHub* represent actionable work that should be done at some future point. It may be as simple as a small product or test bug or as large as the work tracking the design of a new feature. However, it should be work that falls under the charter of Mobius, which is to enable C# binding for Apache Spark. We will keep issues open even if we have no plans to address them in an upcoming milestone, as long as we consider the issue to fall under the charter.

### When we close issues
As noted above, we don't close issues just because we don't plan to address them in an upcoming milestone. So why do we close issues? There are few major reasons:

1. Issues unrelated to Mobius.
2. Nebulous and Large open issues.  Large open issues are sometimes better suited for discussions at mailing lists *[sparkclr-user](https://groups.google.com/d/forum/sparkclr-user)* or *[sparkclr-dev](https://groups.google.com/d/forum/sparkclr-dev)*.

Sometimes after debate, we'll decide an issue isn't a good fit for Mobius.  In that case, we'll also close it.  Because of this, we ask that you don't start working on an issue until it's tagged with *"up for grabs"* or 
*"feature approved"*.  Both you and the team will be unhappy if you spend time and effort working on a change we'll ultimately be unable to take. We try to avoid that.

### Labels
We use GitHub labels to manage workflow on *issues*.  We have the following categories per issue:
* **Area**: These labels call out the feature areas where the issue applies to. 
 * [RDD](https://github.com/Microsoft/SparkCLR/labels/RDD): Issues relating to RDD
 * [DataFrame/SQL](https://github.com/Microsoft/SparkCLR/labels/DataFrame%2FSQL): Issues relating to DataFrame/SQL.
 * [DataFrame UDF](https://github.com/Microsoft/SparkCLR/labels/DataFrame%20UDF): Issues relating to DataFrame UDF.
 * [Streaming](https://github.com/Microsoft/SparkCLR/labels/Streaming): Issues relating to Streaming.
 * [Job Submission](https://github.com/Microsoft/SparkCLR/labels/Job%20Submission): Issues relating to Job Submission.
 * [Packaging](https://github.com/Microsoft/SparkCLR/labels/Packaging): Issues relating to packaging.
 * [Deployment](https://github.com/Microsoft/SparkCLR/labels/Deployment): Issues relating to deployment.
 * [Spark Compatibility](https://github.com/Microsoft/SparkCLR/labels/Spark%20Compatibility): Issues relating to supporting different/newer Apache Spark releases.
* **Type**: These labels classify the type of issue.  We use the following types:
 * [documentation](https://github.com/Microsoft/SparkCLR/labels/documentation): Issues relating to documentation (e.g. incorrect documentation, enhancement requests)
 * [debuggability/supportability](https://github.com/Microsoft/SparkCLR/labels/debuggability%2Fsupportability): Issues relating to making debugging and support easy. For instance, throwing meaningful errors when things fail.
 * [user experience](https://github.com/Microsoft/SparkCLR/labels/user%20experience): Issues relating to making Mobius more user-friendly. For instance, improving the first time user experience, helping run Mobius on a single node or cluster mode etc.
 * [bug](https://github.com/Microsoft/SparkCLR/labels/bug).
 * [enhancement](https://github.com/Microsoft/SparkCLR/labels/enhancement): Issues related to improving existing implementations.
 * [test bug](https://github.com/Microsoft/SparkCLR/labels/test%20bug): Issues related to invalid or missing tests/unit tests.
 * [design change request](https://github.com/Microsoft/SparkCLR/labels/design%20change%20request): Alternative design change suggestions.
 * [suggestion](https://github.com/Microsoft/SparkCLR/labels/suggestion): Feature or API suggestions.
* **Ownership**: These labels are used to specify who owns specific issue. Issues without an ownership tag are still considered "up for discussion" and haven't been approved yet. We have the following different types of ownership:
 * [up for grabs](https://github.com/Microsoft/SparkCLR/labels/up%20for%20grabs): Small sections of work which we believe are well scoped. These sorts of issues are a good place to start if you are new.  Anyone is free to work on these issues.
 * [feature approved](https://github.com/Microsoft/SparkCLR/labels/feature%20approved): Larger scale issues having priority and the design approved, anyone is free to work on these issues, but they may be trickier or require more work.
 * [grabbed by assignee](https://github.com/Microsoft/SparkCLR/labels/grabbed%20by%20assignee): the person the issue is assigned to is making a fix.
* **Project Management**: These labels are used to communicate task status. Issues/tasks without a Project Management tag are still considered as "pendig/under triage".
 * [0 - Backlog](https://github.com/Microsoft/SparkCLR/labels/0%20-%20Backlog): Tasks that are not yet ready for development or are not yet prioritized for the current milestone.
 * [1 - Up Next](https://github.com/Microsoft/SparkCLR/labels/1%20-%20Up%20Next): Tasks that are ready for development and prioritized above the rest of the backlog.
 * [2 - In Progress](https://github.com/Microsoft/SparkCLR/labels/2%20-%20In%20Progress): Tasks that are under active development.
 * [3 - Done](https://github.com/Microsoft/SparkCLR/labels/3%20-%20Done): Tasks that are finished.  There should be no open issues in the Done stage.
* **Review Status**: These labels are used to indicate that the issue/bug cannot be worked on after review. Issues without Review Status, Project Management or Ownership tags are ones pending reviews.
 * [duplicate](https://github.com/Microsoft/SparkCLR/labels/duplicate): Issues/bugs are duplicates of ones submitted already.
 * [invalid](https://github.com/Microsoft/SparkCLR/labels/invalid): Issues/bugs are unrelated to Mobius.
 * [wontfix](https://github.com/Microsoft/SparkCLR/labels/wontfix): Issues/bugs are considered as limitations that will not be fixed.
 * [needs more info](https://github.com/Microsoft/SparkCLR/labels/needs%20more%20info): Issues/bugs need more information. Usually this indicates we can't reproduce a reported bug.  We'll close these issues after a little while if we haven't gotten actionable information, but we welcome folks who have acquired more information to reopen the issue.

In addition to the above, we may introduce new labels to help classify our issues.  Some of these tag may cross cutting concerns (e.g. *performance*, *serialization impact*), where as others are used to help us track additional work needed before closing an issue (e.g. *needs design review*). 

### Prefix titles with labels
Github allows only committers to label issues and pull requests (unfortunately). When creating or updating a Github issue, you can help by prefixing the issue title with proper *Area*, *Type* and other labels in suqare brackets (**[ ]**). After reviewing the issue, we will apply the labels and remove the prefixes from the title. 

Two examples below:
* **[**DataFrame/SQL**]****[**bug**]** DF.showString() throws exception in Sparck 1.5.1 cluster
* **[**Deployment**]****[**bug**]** "sparkclr-submit.cmd --package" throws exception

### Assignee
We will assign each issue to a project member.  In most cases, the assignee will not be the one who ultimately fixes the issue (that only happens in the case where the issue is tagged *"grabbed by assignee"*). The purpose of the assignee is to act as a point of contact between the Mobius project and the community for the issue and make sure it's driven to resolution.  If you're working on an issue and get stuck, please reach out to the assignee (just at mention them)  and they will work to help you out.

======================
**Credit** to the [.Net CoreFx project](https://github.com/dotnet/corefx). We are borrowing liberally from their process.
  
There may be bugs or possible improvements to this page, so help us improve it.
