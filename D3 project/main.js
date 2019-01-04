let store = {};


function loadData() {
    return Promise.all([
        d3.csv("routes.csv"),
        d3.json("countries.geo.json"),
    ]).then(datasets => {
        store.routes = datasets[0];
        store.geoJSON = datasets[1]
        return store;
    })
}


function groupByAirline(data) {
    //Iterate over each route, producing a dictionary where the keys is are the airlines ids and the values are the information of the airline.
    let result = data.reduce((result, d) => {
        let currentData = result[d.AirlineID] || {
            "AirlineID": d.AirlineID,
            "AirlineName": d.AirlineName,
            "Count": 0
        };

        currentData.Count += 1; //TODO: Increment the count (number of routes) of airline.

        result[d.AirlineID] = currentData; //TODO: Save the updated information in the dictionary using the airline id as key.

        return result;
    }, {});

    //We use this to convert the dictionary produced by the code above, into a list, that will make it easier to create the visualization.
    result = Object.keys(result).map(key => result[key]);
    result = result.sort((a,b) => d3.descending(a.Count, b.Count)); //TODO: Sort the data in descending order of count.

    return result
}

function showData() {
    //Get the routes from our store variable
    let routes = store.routes;
    // Compute the number of routes per airline.
    let airlines = groupByAirline(store.routes);
    console.table(airlines);

    drawAirlinesChart(airlines);
    drawMap(store.geoJSON);

    let airports = groupByAirport(store.routes);
    drawAirports(airports)

}

// Running it

loadData().then(showData);


function drawAirlinesChart(airlines) {
    let config = getAirlinesChartConfig()
    let scales = getAirlinesChartScales(airlines, config);
    drawBarsAirlinesChart(airlines, scales, config);
    drawAxesAirlinesChart(airlines, scales, config);
}

function getAirlinesChartConfig() {
    let width = 350;
    let height = 400;
    let margin = {
        top: 10,
        bottom: 50,
        left: 130,
        right: 10
    };
    //The body is the are that will be occupied by the bars.
    let bodyHeight = height - margin.top - margin.bottom;
    let bodyWidth =  width - margin.left - margin.right; //TODO: Compute the width of the body by subtracting the left and right margins from the width.

    //The container is the SVG where we will draw the chart. In our HTML is the svg tah with the id AirlinesChart
    let container = d3.select('#AirlinesChart')//TODO: use d3.select to select the element with id AirlinesChart
    container
        .attr("width", width)
        .attr("height", height);
    //TODO: Set the height of the container

    return { width, height, margin, bodyHeight, bodyWidth, container }
}

function getAirlinesChartScales(airlines, config) {
    let { bodyWidth, bodyHeight } = config;
    let maximumCount = d3.max(airlines, a => a.Count); //TODO: Use d3.max to get the highest Count value we have on the airlines list.
    let minimumCount = d3.min(airlines, a => a.Count);

    let xScale = d3.scaleLinear()
        .domain([0, maximumCount]) //TODO: Set the domain to go from 0 to the maximun value fount for the field 'Count'
        .range([0, bodyWidth]); //TODO: Set the range to go from 0 to the width of the body


    let yScale = d3.scaleBand()
        .range([0, bodyHeight])
        .domain(airlines.map(a => a.AirlineName)) //The domain is the list of airlines names
        .padding(0.2);

    let y2Scale = d3.scaleBand()
        .range([0, bodyHeight])
        .domain(airlines.map(a => a.Count)) //The domain is the list of airlines names
        .padding(0.2);

    color = d3.scaleLinear().domain([minimumCount, maximumCount])
        .interpolate(d3.interpolateHcl)
        .range([d3.rgb("#007AFF"), d3.rgb('#FFF500')]);

    return { xScale, yScale, y2Scale, color }
}

function drawBarsAirlinesChart(airlines, scales, config) {
    let {margin, container} = config; // this is equivalent to 'let margin = config.margin; let container = config.container'
    let {xScale, yScale, _, color} = scales;
    let body = container.append("g")
        .style("transform",
            `translate(${margin.left}px,${margin.top}px)`
        );

    let bars = body.selectAll(".bar").data(airlines)
    //TODO: Use the .data method to bind the airlines to the bars (elements with class bar)


    //Adding a rect tag for each airline
    bars.enter().append("rect")
        .attr("height", yScale.bandwidth())
        .attr("width", (d) => xScale(d.Count))
        .attr("y", (d) => yScale(d.AirlineName))
        //TODO: set the width of the bar to be proportional to the airline count using the xScale
        .attr("fill", (d) => color(d.Count))
        //.attr("fill", (d) => '#'+Math.floor(Math.random()*16777215).toString(16))
        .on("mouseenter", function(d) { // <- this is the new code
            bar = d3.select(this)
            bar.attr('fill', "#992a5b") //TODO: change the fill color of the bar to "#992a5b" as a way to highlight the bar. Hint: use this.style.fill
            return  drawRoutes(d.AirlineID, color(d.Count))
            //TODO: return a call the drawRoutes function passing the AirlineID id 'd'
        }).on("mouseleave", function(d) { // <- this is the new code
            bar = d3.select(this)
            bar.attr('fill', (d) => color(d.Count))
            return drawRoutes(null)
        })

    //Adding a rect tag for each airline
    bars.enter().append("text")
        .text((d) => d.Count)
        .style('font-size', '12px')
        .style('fill', 'white')
        .attr("y", (d) => yScale(d.AirlineName) + 12)
        .attr("x", (d) => config.bodyWidth * .05)
        .on("mouseenter", function(d) { // <- this is the new code
            bar = d3.select(this)
            bar.attr('fill', "#992a5b") //TODO: change the fill color of the bar to "#992a5b" as a way to highlight the bar. Hint: use this.style.fill
            return  drawRoutes(d.AirlineID, color(d.Count))
            //TODO: return a call the drawRoutes function passing the AirlineID id 'd'
        }).on("mouseleave", function(d) { // <- this is the new code
            bar = d3.select(this)
            bar.attr('fill', (d) => color(d.Count))
            return drawRoutes(null)
        })


        //TODO: Add another listener, this time for mouseleave
        //TODO: In this listener, call drawRoutes(null), this will cause the function to remove all lines in the chart since there is no airline withe AirlineID == null.
        //TODO: change the fill color of the bar back to "#2a5599"

}

function drawAxesAirlinesChart(airlines, scales, config){
    let {xScale, yScale, y2Scale} = scales;
    let {container, margin, height} = config;
    let axisX = d3.axisBottom(xScale)
        .ticks(5);

    container.append("g")
        .style("transform",
            `translate(${margin.left}px,${height - margin.bottom}px)`
        )
        .call(axisX);

    let axisY = d3.axisLeft(yScale); //TODO: Create an axis on the left for the Y scale

    //TODO: Append a g tag to the container, translate it based on the margins and call the axisY axis to draw the left axis.
    container.append("g")
        .style("transform",
            `translate(${margin.left}px,${margin.top}px)`
        )
        .call(axisY);

    // let axisY2 = d3.axisLeft(y2Scale); //TODO: Create an axis on the left for the Y scale
    //
    // container.append("g")
    //     .style("transform",
    //         `translate(${margin.left}px,${margin.top}px)`
    //     )
    //     .call(axisY2);
}

function getMapConfig(){
    let width = 600;
    let height = 400;
    let container = d3.select('#Map')//TODO: use d3.select to select the element with id AirlinesChart
    container
        .attr("width", width)
        .attr("height", height);
    //TODO: set the width and height of the conatiner to be equal the width and height variables.
    return {width, height, container}
}


function getMapProjection(config) {
    let {width, height} = config;
    let projection = d3.geoMercator()//TODO: Create a projection of type Mercator.

    projection.scale(97)
        .translate([width / 2, height / 2 + 20])

    store.mapProjection = projection;
    return projection;
}

function drawBaseMap(container, countries, projection){
    let path = d3.geoPath().projection(projection)//TODO: create a geoPath generator and set its projection to be the projection passed as parameter.

        container.selectAll("path").data(countries)
            .enter().append("path")
            .attr("d", path)//TODO: use the path generator to draw each country )
            .attr("stroke", "#ccc")
            .attr("fill", "#eee")
}

function drawMap(geoJeon) {
    let config = getMapConfig();
    let projection = getMapProjection(config)
    drawBaseMap(config.container, geoJeon.features, projection)
}


function groupByAirport(data) {
    //We use reduce to transform a list into a object where each key points to an aiport. This way makes it easy to check if is the first time we are seeing the airport.
    let result = data.reduce((result, d) => {
        //The || sign in the line below means that in case the first option is anything that Javascript consider false (this insclude undefined, null and 0), the second option will be used. Here if result[d.DestAirportID] is false, it means that this is the first time we are seeing the airport, so we will create a new one (second part after ||)

        let currentDest = result[d.DestAirportID] || {
            "AriportID": d.DestAirportID,
            "Airport": d.DestAirport,
            "Latitude": +d.DestLatitude,
            "Longitude": +d.DestLongitude,
            "City": d.DestCity,
            "Country": d.DestCountry,
            "Count": 0
        };
        currentDest.Count += 1;
        result[d.DestAirportID] = currentDest;

        //After doing for the destination airport, we also update the airport the airplane is departing from.
        let currentSource = result[d.SourceAirportID] || {
            "AriportID": d.SourceAirportID,
            "Airport": d.SourceAirport,
            "Latitude": +d.SourceLatitude,
            "Longitude": +d.SourceLongitude,
            "City": d.SourceCity,
            "Country": d.SourceCountry,
            "Count": 0
        };
        currentSource.Count += 1;
        result[d.SourceAirportID] = currentSource;

        return result
    }, {});

    //We map the keys to the actual ariorts, this is an way to transform the object we got in the previous step into a list.
    result = Object.keys(result).map(key => result[key]);
    return result;
}


function drawAirports(airports) {
    let config = getMapConfig(); //get the config
    let projection = getMapProjection(config) //get the projection
    let container = config.container; //get the container

    //TODO: bind the airports to the circles using the .data method.
    let circles = container.selectAll("circle").data(airports);


    circles.enter().append("circle")
        .attr('r', 1)
        .attr('cx', a => projection([a.Longitude, a.Latitude])[0])
        .attr('cy', a => projection([a.Longitude, a.Latitude])[1])
        .attr('fill','#2a5599')
    //TODO: for each new airport (hint: .enter)
    //      - Set the radius to 1
    //      - set the x and y position of the circle using the projection to convert longitude and latitude to x and y porision.
    //      - Set the fill color of the circle to  "#2a5599"

}

function drawRoutes(airlineID, color) {
    let routes = store.routes; //TODO: get the routes from store
    let config = getMapConfig();
    let projection = getMapProjection(config); //TODO: get the projection from the store
    let container = d3.select("#Map"); //TODO: select the svg with id "Map" (our map container)
    let selectedRoutes = routes.filter( r => r.AirlineID == airlineID); //TODO: filter the routes to keep only the routes which AirlineID is equal to the parameter airlineID received by the function

    container.selectAll("line").remove();

    let bindedData = container.selectAll(".lines")
        .data(selectedRoutes, d => d.ID) //This seconf parameter tells D3 what to use to identify the routes, this hepls D3 to correctly find which routes have been added or removed.

    //TODO: Use the .enter selector to append a line for each new route.
    bindedData.enter().append('line')
        .attr('x1', a => projection([a.SourceLongitude, a.SourceLatitude])[0])
        .attr('y1', a => projection([a.SourceLongitude, a.SourceLatitude])[1])
        .attr('x2', a => projection([a.DestLongitude, a.DestLatitude])[0])
        .attr('y2', a => projection([a.DestLongitude, a.DestLatitude])[1])
        .attr('stroke', color)
        .attr('stroke-opacity', 0.1)
        //.exit();
    //TODO: for each line set the start of the line (x1 and y1) to be the position of the source airport (SourceLongitude and SourceLatitude) Hint: you can use projection to convert longitude and latitude to x and y.
    //TODO: for each line set the end of the line (x2 and y2) to be the position of the source airport (DestLongitude and DestLongitude)

    //TODO: set the color of the stroke of the line to "#992a2a"
    //TODO: set the opacity to 0.1

    //TODO: use exit function over bindedData to remove any routes that does not satisfy the filter.
}
