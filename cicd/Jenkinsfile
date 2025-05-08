pipeline {
    agent {
        label 'docker-agent'
    }

    environment {
        DOCKER_HUB = "docker.io"
        DOCKER_REGISTRY = "achodak"
        DOCKER_CREDS = credentials('docker-credentials-id')
        KUBECONFIG = credentials('kubeconfig')
        PROJECT_NAME = "fileprocessing"
        GIT_COMMIT_SHORT = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
        TAG = "${env.BUILD_NUMBER}-${GIT_COMMIT_SHORT}"
    }

    stages {
        
        stage('Build Docker Images') {
            steps {
                container('docker') {
                    script {    
                        withCredentials([usernamePassword(credentialsId: 'docker-credentials-id', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                            sh "docker login -u ${USERNAME} -p ${PASSWORD} ${DOCKER_HUB}"
                        }
                        
                        sh "docker build --network=host -t ${DOCKER_REGISTRY}/${PROJECT_NAME}-api:${TAG} ./api_service"
                        sh "docker build --network=host -t ${DOCKER_REGISTRY}/${PROJECT_NAME}-processor:${TAG} ./processor_service"
                        sh "docker build --network=host -t ${DOCKER_REGISTRY}/${PROJECT_NAME}-notifier:${TAG} ./notification_service"
                    }
                }
            }
        }
        
        stage('Push Docker Images') {
            steps {
                container('docker') {
                    script {
                        withCredentials([usernamePassword(credentialsId: 'docker-credentials-id', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                            sh "echo ${PASSWORD} | docker login ${DOCKER_HUB} -u ${USERNAME} --password-stdin"
                        }
                        
                        sh "docker push ${DOCKER_REGISTRY}/${PROJECT_NAME}-api:${TAG}"
                        sh "docker push ${DOCKER_REGISTRY}/${PROJECT_NAME}-processor:${TAG}"
                        sh "docker push ${DOCKER_REGISTRY}/${PROJECT_NAME}-notifier:${TAG}"
                    }
                }
            }
        }
        stage('Deploy to Kubernetes') {
            steps {
                container('docker') {
                    script {
                        sh '''
                            apk add --no-cache curl
                            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
                            chmod +x kubectl
                            mv kubectl /usr/local/bin/
                            kubectl version --client
                        '''
                        // Apply Kubernetes manifests with variable substitution
                        sh """
                            export KUBECONFIG=${KUBECONFIG}
                            
                            # Create namespace first
                            cat k8s/namespace.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            
                            # Apply other resources with variable substitution
                            cat k8s/configmap.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            cat k8s/secret.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            cat k8s/nfs-pv.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            cat k8s/postgres.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            cat k8s/rabbitmq.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            cat k8s/minio.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                            
                            # Wait for infrastructure to be ready
                            kubectl -n ${PROJECT_NAME} wait --for=condition=ready pod -l app=postgres --timeout=120s
                            kubectl -n ${PROJECT_NAME} wait --for=condition=ready pod -l app=rabbitmq --timeout=120s
                            kubectl -n ${PROJECT_NAME} wait --for=condition=ready pod -l app=minio --timeout=120s
                            
                            # Apply application services with special handling for image tags
                            cat k8s/api.yaml | sed 's|\\\${DOCKER_REGISTRY}|${DOCKER_REGISTRY}|g' | sed 's|\\\${PROJECT_NAME}|${PROJECT_NAME}|g' | sed 's|\\\${TAG}|${TAG}|g' | kubectl apply -f -
                            cat k8s/processor.yaml | sed 's|\\\${DOCKER_REGISTRY}|${DOCKER_REGISTRY}|g' | sed 's|\\\${PROJECT_NAME}|${PROJECT_NAME}|g' | sed 's|\\\${TAG}|${TAG}|g' | kubectl apply -f -
                            cat k8s/notifier.yaml | sed 's|\\\${DOCKER_REGISTRY}|${DOCKER_REGISTRY}|g' | sed 's|\\\${PROJECT_NAME}|${PROJECT_NAME}|g' | sed 's|\\\${TAG}|${TAG}|g' | kubectl apply -f -

                            # Apply ingress last
                            cat k8s/ingress.yaml | sed 's|\${PROJECT_NAME}|${PROJECT_NAME}|g' | kubectl apply -f -
                        """
                    }
                }
            }
        }
    }
    post {
        always {
            // Clean workspace
            cleanWs()
        }
        
        success {
            // Notify on success
            echo "Deployment completed successfully!"
        }
        
        failure {
            // Notify on failure
            echo "Deployment failed!"
        }
    }
}