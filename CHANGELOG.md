## [Unreleased]
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to Semantic Versioning.

## [EMCECS-v0.65.1] - 2021-07-09
### Added
- [13520e6](https://eos2git.cec.lab.emc.com/ECS/flux/commit/13520e695e28ef2036cadfd70d3f1fa3a17f57d0) stdlib: added fillMissing function
- [0b7e74a](https://eos2git.cec.lab.emc.com/ECS/flux/commit/0b7e74a05043034641203d8260fe11cff7345ff8) support backward compatibility for v0.12.0
- [e39cc6a](https://eos2git.cec.lab.emc.com/ECS/flux/commit/e39cc6a605b837d60b2b87deaf60d43a8ecdebce) added feature ecs package with downsampled functions
- [6cd7026](https://eos2git.cec.lab.emc.com/ECS/flux/commit/6cd70260bd84e29638d343b17edc63094daf2f84) added timededup function to ecs package


### Edited/Fixed
- [7a872ff](https://eos2git.cec.lab.emc.com/ECS/flux/commit/7a872ff653d31338d54ddf53a53558fc7b7bc58d) support variable reassignments
- [136f799](https://eos2git.cec.lab.emc.com/ECS/flux/commit/136f7993ead8e0404e9e9fcc7b31aa7b506b9e42) updated ecs functions package
- [2085f68](https://eos2git.cec.lab.emc.com/ECS/flux/commit/2085f68228a7641c607fa0b7f909521c478f32ce) join key checking fixed
- [830f966](https://eos2git.cec.lab.emc.com/ECS/flux/commit/830f966255507be34916e90194d0d9d0019e6496) flux: fixed context canceled issue
- [ebd9f5a](https://eos2git.cec.lab.emc.com/ECS/flux/commit/ebd9f5adb8c0e94eeea389d2ec61602729a94658) flux (window): unexpected row fix
- [a1e413a](https://eos2git.cec.lab.emc.com/ECS/flux/commit/a1e413a48f5bc28b1721d8325bf4d524a297850d) fix timededup panic
- [caf1cc9](https://eos2git.cec.lab.emc.com/ECS/flux/commit/caf1cc92a7e099ea3b95fca88ad7e1eb4d9d6149) added context handling to CSV resultDecoder




## [EMCECS-v0.24.0] - 2019-07-04
### Added
- [470271f](https://eos2git.cec.lab.emc.com/ECS/flux/commit/470271f6fbdf77cdb75ee62a6ddd538ea481abf6) stdlib: added predictLinear function
- [3d87908](https://eos2git.cec.lab.emc.com/ECS/flux/commit/3d879082bd6f78983b46cdb96a6f46e3da128425) flux: added new decoder to decode csv data from channel ([#4](https://eos2git.cec.lab.emc.com/ECS/flux/pull/4))
- [f4ef97e](https://eos2git.cec.lab.emc.com/ECS/flux/commit/f4ef97e7e4ba56ce39dba6a1a464c2889a526b3f) **[MONITORING-290]** Reduce flux memory consumption ([#9](https://eos2git.cec.lab.emc.com/ECS/flux/pull/9))
- [235f52c](https://eos2git.cec.lab.emc.com/ECS/flux/commit/235f52c70544f9d134dd8be7fb3f405f2c95aae5) **[MONITORING-290]** Support for sequential query execution ([#12](https://eos2git.cec.lab.emc.com/ECS/flux/pull/12))
### Edited/Fixed
- [8cf4d06](https://eos2git.cec.lab.emc.com/ECS/flux/commit/8cf4d062c6b9f04faf5554270e1623fe1905fa1d) support non-monotonic values for histogramQuantile
- [f9b5f1f](https://eos2git.cec.lab.emc.com/ECS/flux/commit/f9b5f1f0aac1bf634341eeb1a78c6e1fdda63792) pass staticcheck for predictLinear function
- [6eef2a5](https://eos2git.cec.lab.emc.com/ECS/flux/commit/6eef2a53a0eb746f1adcdab2aee88289260a18a7) fixed code generation
- [e59f4b2](https://eos2git.cec.lab.emc.com/ECS/flux/commit/e59f4b2e16a9c4dda589c7f7a88a708bfde2a359) fixed prefictLinear e2e test
- [91ba185](https://eos2git.cec.lab.emc.com/ECS/flux/commit/91ba18576c3c08b0ca4813810275a45c1c2522f4) Set error if no readers are available ([#5](https://eos2git.cec.lab.emc.com/ECS/flux/pull/5))
- [9a62a0d](https://eos2git.cec.lab.emc.com/ECS/flux/commit/9a62a0dd36e5566805c257b91f27577440fbe81a) **[MONITORING-243]** Fix histogram quantile to return zero if all le are zero ([#7](https://eos2git.cec.lab.emc.com/ECS/flux/pull/7))
- [370b26e](https://eos2git.cec.lab.emc.com/ECS/flux/commit/370b26ebd58a337b67a269c6e06c119994aa80dc) **[MONITORING-243]** Fix histogram quantile ([#8](https://eos2git.cec.lab.emc.com/ECS/flux/pull/8))
- [256efd4](https://eos2git.cec.lab.emc.com/ECS/flux/commit/256efd4f93b3ad8ccbb525256107dc0539b069ca) register toHTTP as usual function without side effect to reduce compilation time ([#11](https://eos2git.cec.lab.emc.com/ECS/flux/pull/11))
- [cacfeeb](https://eos2git.cec.lab.emc.com/ECS/flux/commit/cacfeeb24774206edf1f095d600c04009715170b) fix for tests
- [fbf88c8](https://eos2git.cec.lab.emc.com/ECS/flux/commit/fbf88c89b14179c7e4f045e95b7fc323b4902dae) fix for tests again
- [e4418d6](https://eos2git.cec.lab.emc.com/ECS/flux/commit/e4418d69cc2afd4dcfb9fe0c9191b241ee94fa15) **[MONITORING-290]** Improved flux performance ([#10](https://eos2git.cec.lab.emc.com/ECS/flux/pull/10))
- [8872b75](https://eos2git.cec.lab.emc.com/ECS/flux/commit/8872b75e7fe5cb3d2dec74b93cf579334d15b763) fix for controller test
- [5e7673c](https://eos2git.cec.lab.emc.com/ECS/flux/commit/5e7673c829eb6724ab3a974810745a5a7ca65d71) **[MONITORING-209]** Avoid hung in the flux engine ([#13](https://eos2git.cec.lab.emc.com/ECS/flux/pull/13))
### Removed
- [82948d1](https://eos2git.cec.lab.emc.com/ECS/flux/commit/82948d1aa95b1f08fb7c7c3880471188b3fc41a7)  csv: removed unused statistics usage  