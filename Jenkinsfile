pipeline {
    agent none

    environment {
        AWS_REGION = 'us-east-1'
        AWS_KEYPAIR = credentials('1d028e6f-89ba-4e99-9b79-5bbfcab77abc')
        AWS_ACCESS_KEY_ID = "${env.AWS_KEYPAIR_USR}"
        AWS_SECRET_ACCESS_KEY = "${env.AWS_KEYPAIR_PSW}"
    }

    stages {
        stage('Rubocop') {
            agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

            steps {
                sh 'rake rubocop'
            }
        }
        stage('Unit tests') {
            parallel {
                stage('Ruby 2.5') {
                    agent { dockerfile { filename 'docker/ruby-2.5/Dockerfile' } }

                    steps { sh 'rake spec:unit' }
                }
                stage('Ruby 2.6') {
                    agent { dockerfile { filename 'docker/ruby-2.6/Dockerfile' } }

                    steps { sh 'rake spec:unit' }
                }
                stage('Ruby 2.7') {
                    agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

                    steps { sh 'rake spec:unit' }
                }
            }
        }
        stage('Integration tests') {
            parallel {
                stage('Ruby 2.5') {
                    agent { dockerfile { filename 'docker/ruby-2.5/Dockerfile' } }

                    steps { sh 'rake spec:integration' }
                }
                stage('Ruby 2.6') {
                    agent { dockerfile { filename 'docker/ruby-2.6/Dockerfile' } }

                    steps { sh 'rake spec:integration' }
                }
                stage('Ruby 2.7') {
                    agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

                    steps { sh 'rake spec:integration' }
                }
            }
        }
        stage('Release') {
            agent {
                dockerfile {
                    filename 'docker/ruby-2.7/Dockerfile'
                }
            }

            when { branch 'master' }

            stages {
                stage('Publish') {
                    environment {
                        GEM_HOST_API_KEY = credentials('f34ec285-c262-4ea4-a831-a7a5d77fc41a')
                    }
                    steps {
                        sh 'gem build egis.gemspec --output=bundle.gem'
                        sh 'gem push bundle.gem > push.log || grep \'"code":422\' push.log'
                    }
                }
            }
        }
    }
}
