# Contributing Guidelines

## Reporting Issues

Before creating a new Issue, please check first if a similar Issue [already exists](https://github.com/VolantMQ/volantmq/issues?state=open) or was [recently closed](https://github.com/VolantMQ/volantmq/issues?direction=desc&page=1&sort=updated&state=closed).

[Gophers channel](https://gophers.slack.com/messages/C67KYG1RQ/)

Please provide the following minimum information:
* Your VolantMQ version (or git SHA)
* Your Go version (run `go version` in your console)
* A detailed issue description
* Error Log if present
* If possible, a short example
* Try reproduce issue with development build


## Contributing Code

By contributing to this project, you share your code under the Apache License, Version 2.0, as specified in the LICENSE file.

### Pull Requests Checklist

Please check the following points before submitting your pull request:
- [x] Install [pre-commit](http://pre-commit.com) in your local git copy
      ```
      pre-commit install
      ```
- [x] Code passes all of pre-commit hooks
- [x] Created tests, if possible
- [x] All tests pass
- [x] Extended the README / documentation, if necessary
- [x] Create with remote branch with in following pattern: issue-<issue_id> (e.g. issue-22)

### Code Review

Everyone is invited to review and comment on pull requests.
If it looks fine to you, comment with "LGTM" (Looks good to me).

If changes are required, notice the reviewers with "PTAL" (Please take another look) after committing the fixes.

Before merging the Pull Request, at least one [team member](https://github.com/orgs/VolantMQ/people) must have commented with "LGTM".
