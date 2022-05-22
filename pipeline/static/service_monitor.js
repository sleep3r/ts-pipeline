let target;
let possible_service_statuses = ['up', 'down', 'suspended'];
const jenkins_default_url = 'http://ci.dev.esportmath.com/job/esportmath/';

toastr.options = {
    "closeButton": true,
    "debug": false,
    "newestOnTop": true,
    "progressBar": false,
    "positionClass": "toast-bottom-right",
    "preventDuplicates": false,
    "onclick": null,
    "showDuration": "100",
    "hideDuration": "100",
    "timeOut": "2000",
    "extendedTimeOut": "1000",
    "showEasing": "swing",
    "hideEasing": "linear",
    "showMethod": "fadeIn",
    "hideMethod": "fadeOut",
    "toastClass": "toastr"
};

function send_command(requires_confirmation, command, arguments = null) {
    if (requires_confirmation) {
        if (!confirm('Press OK to confirm ' + command + ' command sending')) {
            return;
        }
    }
    let request = new XMLHttpRequest();
    let params = JSON.stringify({target: target, command: command, command_arguments: arguments});
    request.onload = function () {
        if (request.status === 503) {
            toastr['error']('Unable to send command due to disconnect from global MQ.', 'Hive-side error');
        }
        if (request.status === 200) {
            toastr['success']('Successfully sent ' + command + ' command.', 'Command sent! 🚀')
        }
    };
    request.onerror = () => toastr['error']('Unable to send ' + command + ' command. Can\'t connect to the hive.', 'Sending failed 💩');
    request.open("POST", "/send_command");
    request.setRequestHeader("Content-type", "application/json; charset=utf-8");
    request.send(params);
}

function send_custom_command() {
    let command = document.getElementById('custom-command-name-textarea').value;
    if (command.length === 0) {
        return;
    }
    arguments = document.getElementById('custom-command-args-textarea').value;
    send_command(false, command, arguments);
}

function request_pipelines(onload, onerror) {
    let request = new XMLHttpRequest();
    if (onload !== null) {
        request.onload = onload;
    }
    if (onerror !== null) {
        request.onerror = onerror;
    }
    request.open("GET", "/_pipelines");
    request.send();
}

function fill_popup_with_data(pipeline, service, jenkins_url, last_received_message_time, last_sent_message_time, supported_commands) {
    let last_received_field = document.getElementById('last-received-message-time');
    last_received_field.style.fontFamily = 'Courier';
    last_received_field.textContent = last_received_message_time;

    let last_sent_field = document.getElementById('last-sent-message-time');
    last_sent_field.style.fontFamily = 'Courier';
    last_sent_field.textContent = last_sent_message_time;

    let kibana_url_prefix = 'http://kibana.dev.esportmath.com/app/kibana#/discover?_g=(' +
        'filters:!(),' +
        'refreshInterval:(pause:!t,value:0),' +
        'time:(from:now-3d,to:now))&' +
        '_a=(columns:!(message),' +
        'filters:!(),' +
        'index:\'filebeat-*\',' +
        'interval:h,' +
        'sort:!(\'@timestamp\',desc),';
    let kibana_query_prefix = 'query:(language:kuery,query:\'';
    let kibana_condition_and = '%20AND%20';
    let kibana_container_image_name_condition = 'container.image.name:*' + pipeline + '*';
    let kibana_container_name_condition = 'container.name:*_' + service + '*';
    let kibana_heartbeat_condition = 'NOT%20message:heartbeat*';
    let kibana_query_suffix = '\'))';
    document.getElementById('kibana-link').href = kibana_url_prefix + kibana_query_prefix + kibana_container_image_name_condition
        + ((service == null) ? '' : (kibana_condition_and + kibana_container_name_condition)) + kibana_condition_and
        + kibana_heartbeat_condition + kibana_query_suffix;

    document.getElementById('jenkins-link').href = jenkins_url;

    let custom_commands_list = document.getElementById('custom-commands-list');
    while (custom_commands_list.lastChild) {
        custom_commands_list.removeChild(custom_commands_list.lastChild);
    }
    supported_commands.forEach((command) => {
        let option = document.createElement('option');
        option.value = command;
        custom_commands_list.appendChild(option);
    });

    document.getElementById('custom-command-name-textarea').value = '';
    document.getElementById('custom-command-args-textarea').value = '';

    document.getElementById('service-popup-title').textContent = target.replace('.', '::');
    document.getElementById('pipelines-list').classList.add('smooth-blur-in');
    document.getElementById('pipelines-list').classList.remove('smooth-blur-out');
    document.getElementById('service-popup-background').classList.add('smooth-fade-in');
    document.getElementById('service-popup-background').classList.remove('smooth-fade-out');
    document.getElementById('service-popup').classList.add('smooth-fade-in');
    document.getElementById('service-popup').classList.remove('smooth-fade-out');
}

function show_popup(pipeline, service) {
    target = pipeline + (service === null ? '' : '.' + service);

    if (service === null) {
        request_pipelines(
            onload = (event) => {
                let response = JSON.parse(event.target.response);
                let info = response.info[pipeline];
                let state = response.state[pipeline];
                fill_popup_with_data(pipeline, null, info.jenkins_url, '-', '-', []);
            },
            onerror = () => {
                toastr['error']('Can\'t connect to the hive.', 'Connection lost 😱');
                fill_popup_with_data(pipeline, service, jenkins_default_url, '-', '-', []);
            });
        return
    }
    request_pipelines(
        onload = (event) => {
            let response = JSON.parse(event.target.response);
            let info = response.info[pipeline];
            let state = response.state[pipeline];
            let last_received_message_time = state[service].last_received_message_datetime;
            let last_sent_message_time = state[service].last_sent_message_datetime;
            let supported_commands = info.services[service].supported_commands;
            fill_popup_with_data(pipeline, service, info.jenkins_url, last_received_message_time, last_sent_message_time, supported_commands)
        },
        onerror = () => {
            toastr['error']('Can\'t connect to the hive.', 'Connection lost 😱');
            fill_popup_with_data(pipeline, service, jenkins_default_url,'-', '-', []);
        }
    );
}

function hide_popup() {
    document.getElementById('pipelines-list').classList.add('smooth-blur-out');
    document.getElementById('pipelines-list').classList.remove('smooth-blur-in');
    document.getElementById('service-popup-background').classList.add('smooth-fade-out');
    document.getElementById('service-popup-background').classList.remove('smooth-fade-in');
    document.getElementById('service-popup').classList.add('smooth-fade-out');
    document.getElementById('service-popup').classList.remove('smooth-fade-in');
}

function compile_graph(services, rabbitmq_url) {
    let vertices = Object.keys(services);
    let edges = [];
    vertices.forEach(service_name => {
        let predecessors = services[service_name].predecessors;
        let input_queue_name = services[service_name].input_queue;
        predecessors.forEach(predecessor => {
            edges.push([predecessor, service_name, {
                labelType: 'html',
                label: '<a style="background: rgba(255, 255, 255, 0.8)" href="http://' + rabbitmq_url + '/#/queues/%2F/' + input_queue_name + '" target="_blank">' + input_queue_name + '</a>'
            }]);
        });
    });
    return [vertices, edges];
}

function render_graph(render, parent, nodes, edgeList, pipeline_name, states) {
    let svg = parent.append('svg');
    let inner = svg.append('g');
    let g = new dagreD3.graphlib.Graph().setGraph({});
    nodes.forEach(function (node) {
        g.setNode(node, {label: node, rx: '5', ry: '5'});
    });
    edgeList.forEach((edge) => {
        g.setEdge.apply(g, edge);
    });
    g.graph().rankdir = "LR";
    g.graph().nodesep = 60;
    g.graph().edgesep = 30;
    g.graph().ranksep = 20;
    g.edges().forEach(e => {
        g.edge(e).labeloffset = 0;
    });
    render(inner, g);
    svg.attr('height', g.graph().height).attr('width', g.graph().width);
    svg.selectAll('g.node')
        .on('click', service_name => show_popup(pipeline_name, service_name))
        .style('cursor', 'pointer');
    possible_service_statuses.forEach(status => {
        svg.selectAll('g.node').filter(name => states[name].pipeline_state === status).classed(status, true);
    });
}

function create_pipeline_section(pipeline) {
    let pipeline_name_field = document.createElement('h6');
    pipeline_name_field.textContent = pipeline;

    let pipeline_graph_area = document.createElement('div');
    pipeline_graph_area.id = pipeline + '_graph_area';
    pipeline_graph_area.classList.add('pipeline-graph-area');
    pipeline_graph_area.onclick = (e) => e.stopPropagation();

    let pipeline_section = document.createElement('div');
    pipeline_section.classList.add('pipeline-section');
    pipeline_section.onclick = () => show_popup(pipeline, null);
    pipeline_section.append(pipeline_name_field, pipeline_graph_area);

    let li = document.createElement('li');
    li.appendChild(pipeline_section);
    document.getElementById('pipelines-list').appendChild(li);
}

function fill_services_list() {
    request_pipelines(onload = (event) => {
        let response = JSON.parse(event.target.response);
        let info = response.info;
        let state = response.state;
        Object.keys(info).forEach(pipeline => {
            create_pipeline_section(pipeline);
            let graph = compile_graph(info[pipeline].services, info[pipeline].rabbitmq_url);
            let render = new dagreD3.render();
            render_graph(render, d3.select('#' + pipeline + '_graph_area'), graph[0], graph[1], pipeline, state[pipeline]);
        });
    });
}

fill_services_list();

document.onkeydown = function (evt) {
    evt = evt || window.event;
    if (evt.keyCode === 27) {
        hide_popup();
    }
};

function update_statuses() {
    request_pipelines(onload = (event) => {
            let response = JSON.parse(event.target.response);
            let info = response.info;
            let state = response.state;
            Object.keys(info).forEach(pipeline => {
                let svg = d3.select('#' + pipeline + '_graph_area');
                possible_service_statuses.forEach(status => {
                    svg.selectAll('g.node').classed(status, false);
                    svg.selectAll('g.node').filter(service => state[pipeline][service].pipeline_state === status).classed(status, true);
                });
            });
        },
        onerror = (event) => toastr['error']('Can\'t connect to the hive.', 'Connection lost 😱')
    );
}

setInterval(update_statuses, 5000);