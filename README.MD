# Bitly Coding Challange

## Contents
* [Installation](#installation)
* [Usage](#usage)
* [Design](#design)
* [Caching](#caching)
* [Contributing and Support](#contributing-and-support)
* [License](#license)

## Installation
```
pip install sanic
git clone https://github.com/gnussb/BitlyProject.git

```

## Usage
cd BitlyProject
python3 webserver.py

in your prefered browser open:
http://127.0.0.1:8000/v1/countries/metrics?units=14

## Design

Everything is network bound, so the application so application uses concurrency.

The calculation for getting the nunmber of pages we will need to paginate is done by first getting one page and then simply divding total by links per page.

Once we've aggregated the list of bitlink ids from the previous request, we can schedule for all of the requests to get metric information to happen at once.

