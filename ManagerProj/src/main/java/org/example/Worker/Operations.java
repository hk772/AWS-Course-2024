package org.example.Worker;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.imageio.ImageIO;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

public class Operations {

    public void performOperation(int op, String url, String outputURL) throws IOException {
        // open file as PDDOc
        File file = new File(url);
        PDDocument document = PDDocument.load(file);

        switch (op) {
            case 1:
                this.ToImage(document, outputURL);
                break;
            case 2:
                this.ToHTML(document, outputURL);
                break;
            case 3:
                this.ToText(document, outputURL);
                break;
            default:
                throw new IllegalArgumentException("Invalid operation");
        }
        //close document
        document.close();
    }

    private void ToImage(PDDocument document, String outputURL) throws IOException {
        //perform operation
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0);

        //save
        ImageIO.write(image, "JPEG", new File(outputURL));
    }

    private String getFirstPageTxt(PDDocument document) throws IOException {
        PDFTextStripper pdfStripper = new PDFTextStripper();
        pdfStripper.setStartPage(1);
        pdfStripper.setEndPage(1);
        return pdfStripper.getText(document);
    }

    private void createTxtDocAt(String txt, String url) throws IOException {
        File out = new File(url);
        FileWriter myWriter = new FileWriter(out);
        myWriter.write(txt);
        myWriter.close();
    }

    private void ToHTML(PDDocument document, String outputURL) throws IOException {
        //perform operation
        String content = this.getFirstPageTxt(document);

        String html = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<title>PDF Page 1</title>\n" +
                "</head>\n" +
                "<body>\n";

        html += content + "\n";
        html += "</body>\n</html>";

        //save
        this.createTxtDocAt(html, outputURL);
    }

    private void ToText(PDDocument document, String outputURL) throws IOException {
        //perform operation
        String content = this.getFirstPageTxt(document);

        //save
        this.createTxtDocAt(content, outputURL);
    }


}
