package com.sdf.core.dataflow.lineage

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
// Removed CommandLineRunner as it runs too late for resource handler configuration
// import org.springframework.boot.CommandLineRunner

// This is the main Spring Boot application class
@SpringBootApplication
@Configuration
class StaticFileServer extends WebMvcConfigurer with ApplicationRunner {

  @Value("${server.port:8080}")
  private var serverPort: String = _

  // Inject the external static files path directly from properties or command line
  // The default value is an empty string, but we'll make it mandatory via exception if not provided.
  @Value("${external.static.files.path:}")
  private var externalStaticFilesPath: String = _

  // This method configures how static resources are served
  override def addResourceHandlers(registry: ResourceHandlerRegistry): Unit = {
    // Check if the path was provided via application properties or command line
    if (externalStaticFilesPath.isEmpty) {
      throw new IllegalArgumentException("The 'external.static.files.path' property is required. Please provide it via command line (--external.static.files.path=/path/to/build) or application.properties.")
    }

    val resourcePath = s"file:$externalStaticFilesPath/"
    println(s"Attempting to configure resource handler for: $resourcePath") // This println will now show up

    registry.addResourceHandler("/**")
      .addResourceLocations(resourcePath) // Correctly use addResourceLocations for external paths
      .setCachePeriod(0) // Disable caching for development, remove in production
  }

  // ApplicationRunner's run method is called after context initialization, suitable for final messages
  override def run(args: ApplicationArguments): Unit = {
    println(s"Spring Boot server online at http://localhost:$serverPort/")
    println(s"Serving static content from: $externalStaticFilesPath")
    println("Press Ctrl+C to stop...")
  }

  // Removed the @Bean method and custom handler logic as it was causing the error
}

object SpringBootStaticFileServer {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[StaticFileServer], args: _*)
  }
}