label = "${UUID.randomUUID().toString()}"
git_project = "v3io-py"
git_project_user = "v3io"
git_project_upstream_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"
artifactory_pypi_repo = "https://artifactory.iguazeng.com/artifactory/api/pypi/v3io_pypi"

podTemplate(label: "${git_project}-${label}", inheritFrom: "jnlp-docker-golang-python37") {
    node("${git_project}-${label}") {
        pipelinex = library(identifier: 'pipelinex@development', retriever: modernSCM(
                [$class       : 'GitSCMSource',
                 credentialsId: git_deploy_user_private_key,
                 remote       : "git@github.com:iguazio/pipelinex.git"])).com.iguazio.pipelinex
        common.notify_slack {
            withCredentials([string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')]) {


                github.release(git_deploy_user, git_project, git_project_user, git_project_upstream_user, true, GIT_TOKEN) {
                    RELEASE_ID = github.get_release_id(git_project, git_project_user, "${github.TAG_VERSION}", GIT_TOKEN)
                    parallel(
                            'upload to pypi': {
                                container('python37') {
                                    if( "${github.TAG_VERSION}" != "unstable" ) {
                                        withCredentials([
                                                usernamePassword(credentialsId: "v3io-pypi-credentials", passwordVariable: 'V3IO_PYPI_PASSWORD', usernameVariable: 'V3IO_PYPI_USER'),
                                                usernamePassword(credentialsId: "iguazio-prod-artifactory-credentials", passwordVariable: 'V3IO_ARTIFACTORY_PASSWORD', usernameVariable: 'V3IO_ARTIFACTORY_USER')
                                        ]) {
                                            dir("${github.BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                                try {
                                                    common.shellc("pip install pipenv")
                                                    common.shellc("make update-deps")
                                                    common.shellc("make sync-deps")

                                                    pypi_version = sh(
                                                        script: "echo ${github.DOCKER_TAG_VERSION} | awk -F - '{print \$1}'",
                                                        returnStdout: true
                                                    ).trim()
                                                    common.shellc("ARTIFACTORY_PYPI_URL=${artifactory_pypi_repo} TRAVIS_REPO_SLUG=v3io/v3io-py V3IO_PYPI_USER=${V3IO_PYPI_USER} V3IO_PYPI_PASSWORD=${V3IO_PYPI_PASSWORD} TRAVIS_TAG=${pypi_version} make upload")
                                                } catch (err) {
                                                    unstable("Failed uploading to pypi")
                                                    // Do not continue stages
                                                    throw err
                                                }
                                            }
                                        }
                                    } else {
                                        echo "Uploading to pypi only stable version"
                                    }
                                }
                            }
                    )
                }
            }
        }
    }
}
