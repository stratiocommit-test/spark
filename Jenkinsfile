@Library('libpipelines@master') _

hose {
    MAIL = 'support'
    SLACKTEAM = 'stratiosecurity'
    MODULE = 'spark-krb-dispatcher-support'
    REPOSITORY = 'spark'
    BUILDTOOL = 'make'
    DEVTIMEOUT = 40
    RELEASETIMEOUT = 40
    PKGMODULESNAMES = ['spark-krb-dispatcher-support']

    DEV = { config ->

        doPackage(config)
	    doDocker(config)

     }
}