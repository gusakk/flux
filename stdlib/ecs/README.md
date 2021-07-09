# Adding custom Flux function to 'ecs' package

## Test new function

Custom function can be tested in standalone query. It can be called as if it is from 'ecs' package.

## Add change to flux repository

1. Clone 'EMCECS/flux.git' locally

2. Create new branch and modify ecs.flux file and add verified query there.

3. From stdlib, run `make`. This will update flux_gen.go file.

4. Commit changes to both flux and go files and push to repository

5. Note: always run `make` in `stdlib` before commit and push.

## Add unit tests

1. Work in stdlib directory

2. Edit testing/testdata/ecs.flux

3. Run `make` to recompile tests

4. Run `env GO111MODULE=on go test -tags '' .` to execute stdlib-only tests

#### For code generation you need to have ruby with "good" version of gems
You can install all things locally or just use docker image from circleCI job

```docker run -it -e GOPATH=/tmp/go -e GO111MODULE="on" -v /home/username/go/src/github.com/EMCECS/flux:/flux nathanielc/flux-build```