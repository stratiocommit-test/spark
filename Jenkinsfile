@Library('libpipelines@master') _

hose {
    MAIL = 'support'
    SLACKTEAM = 'stratiosecurity'
    MODULE = 'stratio-spark'
    REPOSITORY = 'spark'
    BUILDTOOL = 'make'
    DEVTIMEOUT = 40
    RELEASETIMEOUT = 40
    PKGMODULESNAMES = ['spark-stratio-develop']

    DEV = { config ->

        doPackage(config)
	doDocker(conf: config, dockerfile:"DockerfileDispatcher")
        doDocker(conf: config, dockerfile:"DockerfileHistory", image:"spark-stratio-history-server-develop")

     }
}
