package edu.sau.pro3;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Set;

@Controller
class PersonnelController {
    private final PersonnelRepository personnel;
    private final EmployeeImages images;
    private final RestClient spark;

    PersonnelController(PersonnelRepository personnel, EmployeeImages images, @Value("${pro3.spark.url}") String sparkUrl) {
        this.personnel = personnel;
        this.images = images;
        var requests = new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3)).build());
        requests.setReadTimeout(Duration.ofSeconds(45));
        this.spark = RestClient.builder().requestFactory(requests).baseUrl(sparkUrl).build();
    }

    @GetMapping("/")
    String index(Model model) {
        model.addAttribute("employees", personnel.directory());
        try {
            model.addAttribute("photoNames", images.photoNames());
        } catch (IOException unavailable) {
            model.addAttribute("photoNames", Set.of());
            model.addAttribute("photosUnavailable", true);
        }
        return "employees";
    }

    @GetMapping("/employees/{empno}")
    String employee(@PathVariable int empno, @RequestParam(defaultValue = "1") int page, Model model) {
        model.addAttribute("personnel", personnel.list());
        model.addAttribute("employee", find(empno));
        model.addAttribute("page", page);
        try {
            var report = readReport(empno, page);
            model.addAttribute("report", report);
            model.addAttribute("hasNext", (long) page * ExpenseReport.PAGE_SIZE < report.count());
        } catch (ResponseStatusException error) {
            if (error.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE) {
                throw error;
            }
            model.addAttribute("reportUnavailable", true);
        }
        return "index";
    }

    @ResponseBody
    @GetMapping("/api/employees/{empno}/expenses")
    ExpenseReport report(@PathVariable int empno, @RequestParam(defaultValue = "1") int page) {
        find(empno);
        return readReport(empno, page);
    }

    @GetMapping("/employees/{empno}/photo")
    ResponseEntity<byte[]> photo(@PathVariable int empno) throws IOException {
        var image = images.read(find(empno).img());
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).contentType(image.mediaType()).body(image.bytes());
    }

    @PostMapping("/employees/{empno}/photo")
    String upload(@PathVariable int empno, @RequestParam("photo") MultipartFile photo, RedirectAttributes redirect)
            throws IOException {
        try {
            images.store(find(empno).img(), photo);
            redirect.addFlashAttribute("photoMessage", "Photo saved to HDFS.");
        } catch (IllegalArgumentException error) {
            redirect.addFlashAttribute("photoError", error.getMessage());
        }
        return "redirect:/employees/" + empno;
    }

    private PersonnelRepository.Employee find(int empno) {
        return personnel.find(empno)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Employee not found"));
    }

    private ExpenseReport readReport(int empno, int page) {
        if (page < 1 || page > Integer.MAX_VALUE / ExpenseReport.PAGE_SIZE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid page number");
        }
        try {
            ExpenseReport report = spark.get().uri("/reports/{empno}/{page}", empno, page)
                    .retrieve().body(ExpenseReport.class);
            if (report != null) {
                return report;
            }
        } catch (RestClientException error) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "The Spark expense report is unavailable", error);
        }
        throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Spark returned an empty response");
    }
}
