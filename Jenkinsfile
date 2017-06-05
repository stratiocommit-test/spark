@Library('libpipelines@master') _

hose {
    MAIL = 'support'
    SLACKTEAM = 'stratiosecurity'
    MODULE = 'spark-stratio'
    REPOSITORY = 'spark'
    BUILDTOOL = 'make'
    DEVTIMEOUT = 40
    RELEASETIMEOUT = 40
    PKGMODULESNAMES = ['spark-stratio-2_11-r2']

    DEV = { config ->

        doPackage(config)
	doDocker(config)

     }
}
