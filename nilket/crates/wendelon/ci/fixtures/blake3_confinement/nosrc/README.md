This tree deliberately has no `src` directory.

`grep` then complains to standard error and finds nothing, and the check
reports success with nothing to check. That is what the Bash original does,
so it is what the port has to do. Without a file here git would not carry the
directory and the case would vanish from a fresh checkout.
