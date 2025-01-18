docker build . -t lambda_berlin
echo "Building the Docker image..."
docker tag lambda_berlin:latest 915485756489.dkr.ecr.us-east-1.amazonaws.com/lambda_berlin:latest
echo "Pushing the Docker image..."
docker push 915485756489.dkr.ecr.us-east-1.amazonaws.com/lambda_berlin:latest
