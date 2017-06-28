@Library('libpipelines@master') _

hose {
    MAIL = 'support'
    SLACKTEAM = 'stratiosecurity'
    MODULE = 'stratio-spark'
    REPOSITORY = 'spark'
    DEVTIMEOUT = 300
    RELEASETIMEOUT = 200
    BUILDTOOLVERSION = '3.5.0'
    PKGMODULESNAMES = ['stratio-spark']


    DEV = { config ->

        doPackage(config)
        //doUT(config)
        doDocker(conf: config, dockerfile:"DockerfileDispatcher")
        doDocker(conf: config, dockerfile:"DockerfileHistory", image:"stratio-spark-history-server")
        doDeploy(config)

     }
}
