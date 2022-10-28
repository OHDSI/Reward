# REal-World Assessment and Research of Drug Performance (Reward)

REal-World Assessment and Research of Drug Performance (reward).

Note: this software is under heavy development and is not recommended for use in production.

# Background - what is Reward?

`Reward` is an R package based around the OMOP CDM and `SelfControlledCohort` OHDSI R HADES package designed to 
generate population level effect estimates at the scale of all drug ingredient exposures by all condition outcomes.
The general purpose of this framework is to generate hypotheses about the unexpected benefits of existing medications.


For executing reward (i.e. you don't want to setup a postgres database) please see 
(`RewardExecutionPackage`) [https://github.com/OHDSI/RewardExecutionPackage] which contains
the only things you need to generate results.

## Installation
Inside an RStudio session, run the following:
```
devtools::install_github("OHDSI/Reward")
```

## Configuring databases
Note: much of this documentation is currently out of date 
```
vignette("Configuring Databases")
```

## Creating a dashboard for study
See the vignette:
```{r}
vignette("dashboards")
```
 
 ## Authors
 Contact James Gilbert (JGilber2@its.jnj.com) or Chris Knoll (cknoll1@its.jnj.com) for more info
 on this software.
