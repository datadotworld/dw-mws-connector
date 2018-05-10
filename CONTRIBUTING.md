# Contributing Guidelines

### General

* Contributions of all kinds (issues, ideas, proposals), not just code, are highly appreciated.
* Pull requests are welcome with the understanding that major changes will be carefully evaluated
and discussed, and may not always be accepted. Starting with a discussion is always best!
* All contributions including documentation, filenames and discussions should be written in the English language.

### Issues

Our [issue tracker](https://github.com/datadotworld/dw-mws-connector/issues) can be used to report issues and
propose changes to the current or next version of this connector.

# Contribute Code

### Relevant Docs

[Amazon MWS API Reference](http://docs.developer.amazonservices.com/en_US/reports/Reports_Overview.html)  
[data.world API Reference](https://apidocs.data.world/)

### Fork the Project

Fork the project [on Github](https://github.com/datadotworld/dw-mws-connector.git) and check out your copy.

```sh
$ git clone https://github.com/[YOUR_GITHUB_NAME]/dw-mws-connector.git
$ cd dw-mws-connector
$ git remote add upstream https://github.com/datadotworld/dw-mws-connector.git
```

### Write Tests

Try to write a test that reproduces the problem you're trying to fix or describes a feature that you want to build.

We definitely appreciate pull requests that highlight or reproduce a problem, even without a fix.

### Write Code

Implement your feature or bug fix. Make sure that all tests pass without errors.

Also, to make sure that your code follows our coding style guide and best practises, run the command;
```sh
$ flake8
```
Make sure to fix any errors that appear, if any.

### Write Documentation

Document any external behavior in the [README](README.md).

### Commit Changes

Make sure git knows your name and email address:

```sh
git config --global user.name "Your Name"
git config --global user.email "contributor@example.com"
```

Writing good commit logs is important. A commit log should describe what changed and why.
```sh
git add ...
git commit
```

### Push

```sh
git push origin my-feature-branch
```

### Make a Pull Request
Go to https://github.com/[YOUR_GITHUB_NAME]/dw-mws-connector.git and select your feature branch. Click the
'Pull Request' button and fill out the form. Pull requests are usually reviewed within a few days.

### Thank you!
Thank you in advance for contributing to this project!
