import urllib2
import urllib
import base64
import json
import ConfigParser
import smtplib
import sys
import time


class RabbitMQManager():
    
    #constructor
    def __init__(self,  username, password , config_filepath):
        self.username = username
        self.password = password
        self.config_filepath = config_filepath
        self.config = ConfigParser.ConfigParser()
        self.config.read( self.config_filepath + "/configfile.ini" )


    #This method takes 3 arguments and 2 are default
    #This method will return json data(except for request_type=delete )
    def callAPI( self, address, request_type = "GET", data = None ):
        try:
            request = urllib2.Request( address, data )

            if request_type == "DELETE":
                request.get_method = lambda : 'DELETE'

            base64string = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
            request.add_header("Authorization", "Basic %s" % base64string)
            response = urllib2.urlopen( request )
            return json.load( response )
        except:
            return False

    #This method doestnot take any argument but it takes the Ip from config file 
    # It return the dictionary having details of node values          
    def getStats( self ):
        stats_dict = {}
        stats_dict[ "has_error" ] = False
        stats_dict[ "error" ] = " "
        try:
            json_data = self.callAPI(self.config.get('url', 'getStats') + "/api/nodes" ) 

             
            stats_dict["file_descriptors"] = json_data[0]['fd_used']
            stats_dict["socket_descriptors"] = json_data[0]['sockets_used']
            stats_dict["erlang_processes"] = json_data[0]['proc_used']
            stats_dict["memory"] = json_data[0]['mem_used']
            stats_dict["disk_space"] = json_data[0]['disk_free']

        except Exception,details:
            stats_dict["error"] = str(details) 
            stats_dict["has_error"] = True
            return stats_dict

        return stats_dict
    


    #This method doestnot take any argument but it takes the Ip from config file 
    #It return the dictionary having details of global count 
    def getStatus( self ):
        status_dict = {}
        status_dict["has_error"] = False
        status_dict["error"] = " "
        try:
            json_data = self.callAPI( self.config.get ('url' , 'getstatus') + "/api/overview" ) 
            
            
            
            global_counts = json_data[ "object_totals" ]
            status_dict[ "connection" ] = global_counts[ "connections" ]
            status_dict[ "channels" ] = global_counts[ "channels" ]
            status_dict[ "queues" ] = global_counts[ "queues" ]
            status_dict[ "consumers" ] = global_counts[ "consumers" ]
            status_dict[ "exchanges" ] = global_counts[ "exchanges" ]
            status_dict[ "message_ready" ] = json_data['queue_totals']['messages_ready']
            status_dict[ "messages_unacknowledged" ] = json_data["queue_totals"]["messages_unacknowledged"]
            status_dict[ "total" ] = json_data['queue_totals']['messages_ready'] + json_data["queue_totals"]["messages_unacknowledged"]

        except Exception,details:
            status_dict[ "error" ] = str(details) 
            status_dict[ "has_error" ] = True
            return status_dict
        

        return status_dict


    #This method takes 2 input vhost and quest name 
    # it deletes the queue and return the dictioary
    def deleteQueue(self , vhost , queue_name):
        delete_queue_dict = {}
        delete_queue_dict[ "has_error" ] = False
        delete_queue_dict[ "error" ] = " "
        try:
            encoded_vhost = urllib.quote_plus( vhost )
            self.callAPI( self.config.get('url', 'deletequeue') + "/api/queues/" + encoded_vhost + "/" + str(queue_name), 'DELETE')
            print(self.config.get('url', 'deletequeue') + "/api/queues/" + encoded_vhost + "/" + str(queue_name))

        except Exception,details:
            delete_queue_dict[ "error" ] = str(details) 
            delete_queue_dict[ "has_error" ] = True
            return delete_queue_dict
        return delete_queue_dict
  
    #This method takes 2 input vhost and quest name and it returns the queue details 
    def queueStatus(self , vhost , queue_name):
        queue_status = {}
        queue_status[ "has_error" ] = False
        queue_status[ "error" ] = " "
        try:
            encoded_vhost = urllib.quote_plus(vhost)
            
            queue_status_data = self.callAPI( self.config.get('url', 'deletequeue' )+ "/api/queues/" + encoded_vhost + "/" + str(queue_name))
            queue_status[ "messages_ready" ] = queue_status_data[ "messages_ready" ]
            queue_status[ "messages_unacknowledged" ] = queue_status_data[ "messages_unacknowledged" ]
            queue_status[ "publish_rate" ] = queue_status_data['messages_details']['rate']
            queue_status[ "total_message" ] = queue_status_data[ "messages_ready" ] + queue_status_data[ "messages_unacknowledged" ]

        except Exception,details:
            queue_status["error"] = str(details) 
            queue_status["has_error"] = True
            return queue_status
        return queue_status


    #This method takes 2 input vhost and quest name and it deletes the contents of queue and returns dictionary
    def queuePurge(self, vhost , queue_name):
        queuePurge_dict = {}
        queuePurge_dict["has_error"] = False
        queuePurge_dict["error"] = " "
        try:
            encoded_vhost = urllib.quote_plus(vhost)
            self.callAPI(self.config.get('url', 'deletequeue') + "/api/queues/"+ encoded_vhost + "/" + str(queue_name) + "/contents", 'DELETE')


        except Exception,details:
            queuePurge_dict["error"] = str(details) 
            queuePurge_dict["has_error"] = True
            return queuePurge_dict
        return queuePurge_dict



    #this method if for closing connection it take inputs from config file     
    def closeConnections( self  , delete_idle_time, username ):
        channels = self.callAPI(self.config.get( 'url', 'closeconnection' ) + '/api/channels' )
        
        
        for channel in channels:
            try:
                current_time = time.time()
                idle_since = time.mktime(time.strptime(channel['idle_since'], '%Y-%m-%d %H:%M:%S'))
                seconds = current_time - idle_since
                quoted_query = urllib.quote(channel['connection_details']['name'])
                url = self.config.get( 'url', 'closeconnection' ) + '/api/connections/' + quoted_query
            except:
                continue

            if delete_idle_time  and username: 
                try:
                    if seconds > delete_idle_time and channel['user'] == username:
                        
                        self.callAPI( url, 'DELETE')
                    else:
                        continue
                         
                except Exception,detail:
                    print(detail)

            elif delete_idle_time  and not username :
                try:
                    if seconds > float(delete_idle_time):
                        self.callAPI( url, 'DELETE')
                    else:
                        continue
                        
                except Exception,detail:
                    print( detail)

            elif username and not delete_idle_time:
                try:
                    
                    if channel['user'] == username:
                        self.callAPI( url , 'DELETE')
                    else:
                        continue
                except Exception,detail:
                    print(detail)

            else:
                try:
                    self.callAPI( url, 'DELETE')
                except Exception,details:
                    continue

    #This method takes 2 arguments as dictionary
    #first argument is the output dict data by any method and second is the test dictionary
    def alert(self , alert_check_data , alert_data ):
        message = ""
        for key in alert_data:
            try:
                check_data = alert_check_data[key]
                condition = alert_data[key]["condition"]
                alert_value = alert_data[key]["value"]
                command = str(alert_value) + str(condition)+str(check_data)
                q = eval(command)
                
                if not q:
                    message =  message + "limit excedded for" + key + "\n"
            except:
                message = message + "Inavalid parametr provided as " + key + "\n"

        print(message)   

        


a = RabbitMQManager("linkex", "linkex","/home/shubham")







