///
/// main.cpp
///
/// Created on: 22 мая 2015 г.
///     Author: alexen
///

#include <hiredis/hiredis.h>

#include <memory>
#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/format.hpp>

using redis_ctx_unique_ptr = std::unique_ptr< redisContext, void(*)( redisContext* ) >;


void print( boost::mutex& printMutex, const std::string& threadName, const boost::format& fmt, std::ostream& ostr = std::cout )
{
     boost::lock_guard< boost::mutex > lock( printMutex );
     ostr << "[th." << boost::this_thread::get_id() << " (" << threadName << ")]: " << fmt << '\n';
}


void printRedisReply( boost::mutex& printMutex, const std::string& threadName, const redisReply* reply )
{
     BOOST_ASSERT( reply );

     switch( reply->type )
     {
          case REDIS_REPLY_STATUS:
               print( printMutex, threadName, boost::format( "REPLY: status: %1%" ) % std::string( reply->str, reply->len ) );
               break;

          case REDIS_REPLY_INTEGER:
               print( printMutex, threadName, boost::format( "REPLY: integer: %1%" ) % reply->integer );
               break;

          case REDIS_REPLY_STRING:
               print( printMutex, threadName, boost::format( "REPLY: string: %1%" ) % std::string( reply->str, reply->len ) );
               break;

          case REDIS_REPLY_ARRAY:
               print( printMutex, threadName, boost::format( "REPLY: array of size %1%" ) % reply->elements );
               for( decltype( reply->elements ) i = 0; i < reply->elements; ++i )
                    printRedisReply( printMutex, threadName, reply->element[ i ] );
               break;

          case REDIS_REPLY_ERROR:
               print( printMutex, threadName, boost::format( "REPLY: error: %1%" ) % std::string( reply->str, reply->len ) );
               throw std::runtime_error( std::string( reply->str, reply->len ) );

          case REDIS_REPLY_NIL:
               print( printMutex, threadName, boost::format( "REPLY: nil (no data)" ) );
               throw std::runtime_error( "no more data" );
     }
}


void producer( boost::mutex& printMutex, const std::string& threadName, redis_ctx_unique_ptr ctx, const std::string& listName, int entries )
{
     try
     {
          print( printMutex, threadName, boost::format( "producer thread started" ) );

          for( int i = 0; i < entries; ++i )
          {
               const redisReply* reply =
                    static_cast< redisReply* >( redisCommand( ctx.get(), "LPUSH %s %d", listName.c_str(), i + 1 ) );

               if( !reply )
                    throw std::runtime_error( ctx->errstr );
          }
     }
     catch( const std::exception& e )
     {
          print( printMutex, threadName, boost::format( "exception: %1%" ) % e.what(), std::cerr );
     }
}


void consumer( boost::mutex& printMutex, const std::string& threadName, redis_ctx_unique_ptr ctx, const std::string& listName, int timeout )
{
     try
     {
          print( printMutex, threadName, boost::format( "consumer thread started" ) );

          while( true )
          {
               const redisReply* reply =
                    static_cast< redisReply* >( redisCommand( ctx.get(), "BLPOP %s %d", listName.c_str(), timeout ) );

               if( !reply )
                    throw std::runtime_error( ctx->errstr );

               if( reply->type == REDIS_REPLY_ERROR )
                    throw std::runtime_error( std::string( reply->str, reply->len ) );

               if( reply->type == REDIS_REPLY_NIL )
                    throw std::runtime_error( "no more data" );

               if( reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 )
               {
                    if( reply->element[ 0 ]->type == REDIS_REPLY_STRING && reply->element[ 1 ]->type == REDIS_REPLY_STRING )
                    {
                         if( reply->element[ 0 ]->str == listName )
                         {
                              print( printMutex, threadName, boost::format( "processing information %1%" ) % reply->element[ 1 ]->str );
                         }
                         else
                              print( printMutex, threadName, boost::format( "unexpected list name: ignored" ) );

                    }
                    else
                         print( printMutex, threadName, boost::format( "unexpected element type inside reply: ignored" ) );

               }
               else
                    print( printMutex, threadName, boost::format( "unexpected reply content: ignored" ) );

               boost::this_thread::sleep_for( boost::chrono::milliseconds( 400 ) );
          }
     }
     catch( const std::exception& e )
     {
          print( printMutex, threadName, boost::format( "exception: %1%" ) % e.what(), std::cerr );
     }
}


redis_ctx_unique_ptr createRedisContext( const std::string& host, int port )
{
     auto ctx = redisConnect( host.c_str(), port );

     if( !ctx )
          throw std::runtime_error( "cannot initialize redis context" );

     if( ctx && ctx->err )
          throw std::runtime_error( ctx->errstr );

     return std::unique_ptr< redisContext, void(*)( redisContext* ) >( ctx, redisFree );
}


int main( int argc, char** argv )
{
     try
     {
          namespace po = boost::program_options;

          std::string host;
          int port = -1;
          int threads = 4;
          std::string listName = "delays";
          int timeout = 5;
          int entries = 10;

          po::options_description descr( "Options" );
          descr.add_options()
               ( "help", "show this help message" )
               ( "host,h", po::value< std::string >( &host )->default_value( "127.0.0.1" ), "redis server host" )
               ( "port,p", po::value< int >( &port )->default_value( 6379 ), "redis server port" )
               ( "threads,n", po::value< int >( &threads )->default_value( threads ), "consumer threads number" )
               ( "list-name,l", po::value< std::string >( &listName )->default_value( "delays" ), "blocking list name in redis" )
               ( "timeout,t", po::value< int >( &timeout )->default_value( timeout ), "block timeout (in seconds)" )
               ( "entries,e", po::value< int >( &entries )->default_value( entries ), "producer's list length" )
          ;

          po::variables_map vm;
          po::store( po::parse_command_line( argc, argv, descr ), vm );
          po::notify( vm );

          if( vm.count( "help" ) )
          {
               std::cout << descr << std::endl;
               return 0;
          }

          boost::mutex printMutex;
          boost::thread_group tg;

          tg.create_thread(
               boost::bind(
                    producer,
                    boost::ref( printMutex ),
                    "producer",
                    boost::bind( createRedisContext, boost::cref( host ), port ),
                    boost::cref( listName ),
                    entries
               )
          );

          for( int i = 0; i < threads; ++i )
          {
               tg.create_thread(
                    boost::bind(
                         consumer,
                         boost::ref( printMutex ),
                         "consumer-" + boost::lexical_cast< std::string >( i + 1 ),
                         boost::bind( createRedisContext, boost::cref( host ), port ),
                         boost::cref( listName ),
                         timeout
                    )
               );
          }

          tg.join_all();
     }
     catch( const std::exception& e )
     {
          std::cerr << "exception: "
               << boost::diagnostic_information( e )
               << '\n';
     }

     return 0;
}
