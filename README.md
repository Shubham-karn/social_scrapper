# Get social media data

This repository provides data on what's trending on social media, who the top influencers are, and includes top and recent posts. Additionally, it offers the latest and categorized news from Singapore.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- You are using a Linux machine.
- You have `sudo` privileges.

## Installing Docker

1. **Update your existing list of packages:**

    ```sh
    sudo apt update
    ```

2. **Install a few prerequisite packages which let `apt` use packages over HTTPS:**

    ```sh
    sudo apt install apt-transport-https ca-certificates curl software-properties-common
    ```

3. **Add the GPG key for the official Docker repository to your system:**

    ```sh
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    ```

4. **Add the Docker repository to APT sources:**

    ```sh
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    ```

5. **Update the package database with Docker packages from the newly added repo:**

    ```sh
    sudo apt update
    ```

6. **Install Docker:**

    ```sh
    sudo apt install docker-ce
    ```

7. **Check Docker status:**

    ```sh
    sudo systemctl status docker
    ```

## Installing Docker Compose

1. **Download the latest version of Docker Compose:**

    ```sh
    sudo curl -L "https://github.com/docker/compose/releases/download/$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -Po '"tag_name": "\K.*?(?=")')/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    ```

2. **Apply executable permissions to the binary:**

    ```sh
    sudo chmod +x /usr/local/bin/docker-compose
    ```

3. **Verify that the installation was successful:**

    ```sh
    docker-compose --version
    ```

## Running the Application

1. **Clone the repository:**

    ```sh
    git clone https://github.com/Shubham-karn/social_scrapper.git
    cd social_scrapper
    ```

2. **Build and run the Docker containers:**

    ```sh
    docker-compose up --build -d
    ```

3. **Check the status of your containers:**

    ```sh
    docker-compose ps
    ```

## Usage

Instructions on how to use your application.

## Contributing

To contribute to this project, please follow these steps:

1. Fork this repository.
2. Create a branch: `git checkout -b feature/your-feature`.
3. Make your changes and commit them: `git commit -m 'Add some feature'`.
4. Push to the original branch: `git push origin feature/your-feature`.
5. Create the pull request.

Alternatively, see the GitHub documentation on [creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

## License

This project uses the following license: [MIT License](LICENSE).

## Contact

If you want to contact me, you can reach me at [shubhamkarn750@gmail.com](mailto:your-email@example.com).
