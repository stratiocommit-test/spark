@Library('libpipelines@master') _

hose {
    MAIL = 'support'
    SLACKTEAM = 'stratiosecurity'
    MODULE = 'spark-krb-calico'
    REPOSITORY = 'spark'
    BUILDTOOL = 'make'
    DEVTIMEOUT = 40
    RELEASETIMEOUT = 40
    PKGMODULESNAMES = ['spark-krb-calico']

    DEV = { config ->

        doPackage(config)
	doDocker(config)

     }
}
