pipeline {
    agent none

    stages {
        stage('Rubocop') {
            agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

            steps {
                sh 'bundle exec rake rubocop'
            }
        }
        stage('Unit tests') {
            parallel {
                stage('Ruby 2.5') {
                    agent { dockerfile { filename 'docker/ruby-2.5/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:unit' }
                }
                stage('Ruby 2.6') {
                    agent { dockerfile { filename 'docker/ruby-2.6/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:unit' }
                }
                stage('Ruby 2.7') {
                    agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:unit' }
                }
            }
        }
        stage('Integration tests') {
            environment {
                AWS_REGION = 'us-east-1'
                AWS_KEYPAIR = credentials('1d028e6f-89ba-4e99-9b79-5bbfcab77abc')
                AWS_ACCESS_KEY_ID = "${env.AWS_KEYPAIR_USR}"
                AWS_SECRET_ACCESS_KEY = "${env.AWS_KEYPAIR_PSW}"
            }
            parallel {
                stage('Ruby 2.5') {
                    agent { dockerfile { filename 'docker/ruby-2.5/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:integration' }
                }
                stage('Ruby 2.6') {
                    agent { dockerfile { filename 'docker/ruby-2.6/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:integration' }
                }
                stage('Ruby 2.7') {
                    agent { dockerfile { filename 'docker/ruby-2.7/Dockerfile' } }

                    steps { sh 'bundle exec rake spec:integration' }
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
                stage('Bundle') {
                    steps {
                        sh 'gem build aegis.gemspec --output=bundle.gem'
                    }
                }
                stage('Publish') {
                    environment {
                        GEM_HOST_API_KEY = credentials('f1466a06-4751-4a7c-83c0-3b591946e0e2')
                    }
                    steps {
                        sh '''
                            gem push bundle.gem --key gemstash --host http://gemstash.talkwit.tv/private > push.log || \
                            grep '"code":422' push.log
                        '''
                    }
                }
            }
        }
    }
}
